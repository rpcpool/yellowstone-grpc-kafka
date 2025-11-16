use {
    anyhow::Context,
    clap::Parser,
    futures::stream::StreamExt,
    redis::{aio::ConnectionManager, AsyncCommands},
    std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration},
    tokio::sync::RwLock,
    tonic::transport::ClientTlsConfig,
    tracing::{info, warn, error, debug},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_kafka::{
        config::{ConfigGrpcRequest, GrpcRequestToProto},
        create_shutdown,
        metrics::run_server as prometheus_run_server,
        setup_tracing,
    },
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, 
        SubscribeUpdate,
        SubscribeUpdateAccount,
        SubscribeUpdateSlot,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Solana Account/Slot Processor with Redis Coordination")]
struct Args {
    /// RPC endpoint (e.g., https://magicede-magicede-c0f1.mainnet.rpcpool.com:443)
    #[clap(long, env = "REQUEST_ENDPOINT")]
    endpoint: String,

    /// X-Token for authentication
    #[clap(long, env = "REQUEST_X_TOKEN")]
    x_token: Option<String>,

    /// Request body (JSON string with accounts/slots config)
    #[clap(long, env = "REQUEST_BODY")]
    request_body: String,

    /// Prometheus listen address
    #[clap(long, env = "PROMETHEUS_ADDRESS", default_value = "0.0.0.0:9090")]
    prometheus_address: SocketAddr,

    /// Provider name (e.g., "triton", "magiceden")
    #[clap(long, env = "PROVIDER_NAME", default_value = "unknown")]
    provider_name: String,

    /// Redis URL for slot coordination
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6379")]
    redis_url: String,

    /// Slot ownership TTL in seconds (default 300 = 5 minutes)
    #[clap(long, env = "SLOT_OWNERSHIP_TTL_SECS", default_value = "300")]
    slot_ownership_ttl_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing();

    let args = Args::parse();
    info!("Starting Solana account processor for provider: {}", args.provider_name);
    info!("Connecting to: {}", args.endpoint);

    // Start Prometheus metrics server
    tokio::spawn(prometheus_run_server(args.prometheus_address));

    // Create shutdown handler
    let shutdown = create_shutdown()?;

    // Run the processor
    run_processor(args, shutdown).await
}

async fn run_processor(
    args: Args,
    mut shutdown: futures::future::BoxFuture<'static, ()>,
) -> anyhow::Result<()> {
    // Connect to Redis
    info!("Connecting to Redis at: {}", args.redis_url);
    let redis_client = redis::Client::open(args.redis_url.as_str())
        .context("failed to create redis client")?;
    let redis_conn = ConnectionManager::new(redis_client)
        .await
        .context("failed to connect to redis")?;
    info!("✅ Connected to Redis");

    // Parse the request body
    let request: ConfigGrpcRequest = serde_json::from_str(&args.request_body)
        .context("failed to parse REQUEST_BODY")?;

    info!("Subscribing to accounts: {:?}", request.accounts.keys().collect::<Vec<_>>());
    info!("Subscribing to slots: {:?}", request.slots.keys().collect::<Vec<_>>());

    // Connect to gRPC
    let mut client_builder = GeyserGrpcClient::build_from_shared(args.endpoint.clone())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(5))
        .tls_config(ClientTlsConfig::new().with_native_roots())?;

    if let Some(token) = args.x_token {
        client_builder = client_builder.x_token(token)?;
    }

    let mut client = client_builder.connect().await?;
    let mut geyser = client.subscribe_once(request.to_proto()).await?;

    info!("✅ Connected to {} and subscribed!", args.provider_name);

    // Initialize your custom processor with Redis
    let processor = Arc::new(RwLock::new(
        SolanaAccountProcessor::new(
            args.provider_name,
            redis_conn,
            args.slot_ownership_ttl_secs,
        )
    ));

    let mut messages_read = 0u64;

    // Main processing loop
    loop {
        let message = tokio::select! {
            _ = &mut shutdown => {
                warn!("Shutdown signal received");
                break;
            }
            message = geyser.next() => message,
        }
        .transpose()?;

        match message {
            Some(message) => {
                messages_read += 1;

                if messages_read % 1000 == 0 {
                    info!("Processed {} messages", messages_read);
                }

                match &message.update_oneof {
                    Some(UpdateOneof::Account(account_update)) => {
                        let mut proc = processor.write().await;
                        if let Err(e) = proc.handle_account_update(account_update).await {
                            error!("Error processing account: {:?}", e);
                        }
                    }
                    Some(UpdateOneof::Slot(slot_update)) => {
                        let mut proc = processor.write().await;
                        if let Err(e) = proc.handle_slot_update(slot_update).await {
                            error!("Error processing slot: {:?}", e);
                        }
                    }
                    Some(UpdateOneof::Transaction(tx_update)) => {
                        debug!("Received transaction at slot {}", tx_update.slot);
                        // Add transaction handling if needed
                    }
                    Some(UpdateOneof::Ping(_)) => {
                        info!("Received ping (messages_read: {})", messages_read);
                    }
                    Some(UpdateOneof::Pong(_)) => {
                        debug!("Received pong");
                    }
                    _ => {}
                }
            }
            None => {
                warn!("Stream ended");
                break;
            }
        }
    }

    info!("Shutting down after processing {} messages", messages_read);
    Ok(())
}

/// Solana Account Processor - implements business logic with Redis coordination
struct SolanaAccountProcessor {
    provider_name: String,
    redis_conn: ConnectionManager,
    slot_ownership_ttl_secs: u64,
    latest_slot: u64,
    account_updates_count: u64,
    slot_updates_count: u64,
    account_updates_processed: u64,
    account_updates_skipped: u64,
    // Local cache of owned slots (for fast lookup)
    owned_slots: HashMap<u64, bool>,
    // Add your custom state here
    // account_cache: HashMap<Vec<u8>, AccountData>,
    // pending_accounts_by_slot: HashMap<u64, Vec<AccountUpdate>>,
    // etc.
}

impl SolanaAccountProcessor {
    fn new(
        provider_name: String,
        redis_conn: ConnectionManager,
        slot_ownership_ttl_secs: u64,
    ) -> Self {
        Self {
            provider_name,
            redis_conn,
            slot_ownership_ttl_secs,
            latest_slot: 0,
            account_updates_count: 0,
            slot_updates_count: 0,
            account_updates_processed: 0,
            account_updates_skipped: 0,
            owned_slots: HashMap::new(),
        }
    }

    /// Try to claim ownership of a slot in Redis
    /// Returns true if this processor now owns the slot
    async fn try_claim_slot(&mut self, slot: u64) -> anyhow::Result<bool> {
        let key = format!("slot:{}", slot);
        
        // Try to set the key only if it doesn't exist (NX)
        // SET key value NX EX ttl
        let result: Option<String> = self
            .redis_conn
            .clone()
            .set_options(
                &key,
                &self.provider_name,
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::EX(self.slot_ownership_ttl_secs as usize)),
            )
            .await?;

        let owned = result.is_some();
        
        if owned {
            self.owned_slots.insert(slot, true);
            debug!("[{}] 🔒 Claimed ownership of slot {}", self.provider_name, slot);
        } else {
            // Check who owns it
            let owner: Option<String> = self.redis_conn.clone().get(&key).await?;
            debug!(
                "[{}] ⏭️  Slot {} already owned by {:?}",
                self.provider_name, slot, owner
            );
        }

        Ok(owned)
    }

    /// Check if this processor owns a slot (check cache first, then Redis)
    async fn owns_slot(&mut self, slot: u64) -> anyhow::Result<bool> {
        // Check local cache first
        if let Some(&owned) = self.owned_slots.get(&slot) {
            return Ok(owned);
        }

        // Not in cache, check Redis
        let key = format!("slot:{}", slot);
        let owner: Option<String> = self.redis_conn.clone().get(&key).await?;
        
        let owned = owner.as_ref() == Some(&self.provider_name);
        
        // Cache the result
        self.owned_slots.insert(slot, owned);
        
        Ok(owned)
    }

    /// Clean up old entries from local cache (keep last 1000 slots)
    fn cleanup_cache(&mut self) {
        if self.owned_slots.len() > 1000 {
            let min_slot = self.latest_slot.saturating_sub(1000);
            self.owned_slots.retain(|&slot, _| slot >= min_slot);
        }
    }

    async fn handle_account_update(
        &mut self,
        update: &SubscribeUpdateAccount,
    ) -> anyhow::Result<()> {
        self.account_updates_count += 1;

        // 🔑 CHECK SLOT OWNERSHIP - Only process if we own this slot
        let owns = self.owns_slot(update.slot).await?;
        
        if !owns {
            self.account_updates_skipped += 1;
            if self.account_updates_skipped % 100 == 0 {
                debug!(
                    "[{}] ⏭️  Skipped {} account updates (not slot owner)",
                    self.provider_name,
                    self.account_updates_skipped
                );
            }
            return Ok(());
        }

        // ✅ We own this slot - process the account update!
        self.account_updates_processed += 1;

        // 🎯 YOUR CUSTOM LOGIC HERE
        
        // Example: Log account update details
        if self.account_updates_processed % 100 == 0 {
            info!(
                "[{}] ✅ Account update #{} at slot {} (pubkey: {}...) [processed: {}, skipped: {}]",
                self.provider_name,
                self.account_updates_count,
                update.slot,
                bs58::encode(&update.account.as_ref().unwrap().pubkey[..8]).into_string(),
                self.account_updates_processed,
                self.account_updates_skipped,
            );
        }

        // Example use cases:
        // 1. Write to database
        // 2. Update in-memory cache
        // 3. Compute state changes
        // 4. Trigger webhooks
        // 5. Update aggregations
        // 6. Feed into ML pipeline
        // etc.

        Ok(())
    }

    async fn handle_slot_update(
        &mut self,
        update: &SubscribeUpdateSlot,
    ) -> anyhow::Result<()> {
        self.slot_updates_count += 1;
        self.latest_slot = update.slot;

        // 🔑 TRY TO CLAIM OWNERSHIP OF THIS SLOT
        let claimed = self.try_claim_slot(update.slot).await?;

        // Clean up old cache entries periodically
        if self.slot_updates_count % 100 == 0 {
            self.cleanup_cache();
        }

        // 🎯 YOUR CUSTOM LOGIC HERE

        // Example: Log slot progression
        if self.slot_updates_count % 100 == 0 {
            let ownership_rate = if self.slot_updates_count > 0 {
                (self.owned_slots.values().filter(|&&v| v).count() as f64 / self.owned_slots.len() as f64) * 100.0
            } else {
                0.0
            };

            info!(
                "[{}] Slot update #{} - slot: {} (parent: {}) | Ownership: {:.1}% | Accounts processed: {}, skipped: {}",
                self.provider_name,
                self.slot_updates_count,
                update.slot,
                update.parent.unwrap_or(0),
                ownership_rate,
                self.account_updates_processed,
                self.account_updates_skipped,
            );
        }

        if claimed {
            debug!("[{}] 🎯 Claimed slot {} - will process its accounts", self.provider_name, update.slot);
        }

        // Example use cases:
        // 1. Track slot progression
        // 2. Detect reorgs (slot goes backward)
        // 3. Trigger batch processing when slot is confirmed
        // 4. Update watermarks
        // 5. Correlate with pending account updates
        // etc.

        Ok(())
    }
}

