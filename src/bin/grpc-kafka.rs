use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::{future::BoxFuture, stream::StreamExt, SinkExt},
    rdkafka::{config::ClientConfig, consumer::Consumer, message::Message, producer::FutureRecord},
    sha2::{Digest, Sha256},
    std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration},
    tokio::task::JoinSet,
    tokio_tungstenite::{connect_async, tungstenite::protocol::Message as TokioMessage},
    tonic::transport::ClientTlsConfig,
    tracing::{debug, trace, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_kafka::{
        config::GrpcRequestToProto,
        create_shutdown,
        env::{load_grpc2kafka_config, load_wss2kafka_config},
        health::{ack_ping, ack_pong},
        kafka::{
            config::{Config, ConfigDedup, ConfigGrpc2Kafka, ConfigKafka2Grpc, ConfigWss2Kafka},
            dedup::KafkaDedup,
            grpc::GrpcService,
            metrics,
            wss::WssEvent,
        },
        metrics::{run_server as prometheus_run_server, GprcMessageKind},
        setup_tracing,
    },
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message as _,
    },
};

// Helper function to extract slot from UpdateOneof
fn extract_slot_from_update(update_oneof: &UpdateOneof) -> Option<u64> {
    match update_oneof {
        UpdateOneof::Account(msg) => Some(msg.slot),
        UpdateOneof::Slot(msg) => Some(msg.slot),
        UpdateOneof::Transaction(msg) => Some(msg.slot),
        UpdateOneof::TransactionStatus(msg) => Some(msg.slot),
        UpdateOneof::Block(msg) => Some(msg.slot),
        UpdateOneof::BlockMeta(msg) => Some(msg.slot),
        UpdateOneof::Entry(msg) => Some(msg.slot),
        UpdateOneof::Ping(_) | UpdateOneof::Pong(_) => None,
    }
}

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about = "Yellowstone gRPC Kafka Tool")]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: Option<String>,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// Receive data from Kafka, deduplicate and send them back to Kafka
    Dedup,
    /// Receive data from gRPC and send them to the Kafka
    #[command(name = "grpc2kafka")]
    Grpc2Kafka,
    /// Receive data from Kafka and send them over gRPC
    #[command(name = "kafka2grpc")]
    Kafka2Grpc,
    /// Receive data from websocket and send them to the Kafka
    #[command(name = "wss2kafka")]
    Wss2Kafka,
}

impl ArgsAction {
    fn load_config(&self) -> anyhow::Result<Config> {
        match self {
            ArgsAction::Dedup => Err(anyhow::anyhow!("`dedup` env is not supported")),
            ArgsAction::Grpc2Kafka => load_grpc2kafka_config(),
            ArgsAction::Kafka2Grpc => Err(anyhow::anyhow!("`kafka2grpc` env is not supported")),
            ArgsAction::Wss2Kafka => load_wss2kafka_config(),
        }
    }

    async fn run(self, config: Config, kafka_config: ClientConfig) -> anyhow::Result<()> {
        let shutdown = create_shutdown()?;
        match self {
            ArgsAction::Dedup => {
                let config = config.dedup.ok_or_else(|| {
                    anyhow::anyhow!("`dedup` section in config should be defined")
                })?;
                Self::dedup(kafka_config, config, shutdown).await
            }
            ArgsAction::Grpc2Kafka => {
                let config = config.grpc2kafka.ok_or_else(|| {
                    anyhow::anyhow!("`grpc2kafka` section in config should be defined")
                })?;
                Self::grpc2kafka(kafka_config, config, shutdown).await
            }
            ArgsAction::Kafka2Grpc => {
                let config = config.kafka2grpc.ok_or_else(|| {
                    anyhow::anyhow!("`kafka2grpc` section in config should be defined")
                })?;
                Self::kafka2grpc(kafka_config, config, shutdown).await
            }
            ArgsAction::Wss2Kafka => {
                let config = config.wss2kafka.ok_or_else(|| {
                    anyhow::anyhow!("`wss2kafka` section in config should be defined")
                })?;
                Self::wss2kafka(kafka_config, config, shutdown).await
            }
        }
    }

    async fn dedup(
        mut kafka_config: ClientConfig,
        config: ConfigDedup,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // input
        let (consumer, kafka_error_rx1) =
            metrics::StatsContext::create_stream_consumer(&kafka_config)
                .context("failed to create kafka consumer")?;
        consumer.subscribe(&[&config.kafka_input])?;

        // output
        let (kafka, kafka_error_rx2) = metrics::StatsContext::create_future_producer(&kafka_config)
            .context("failed to create kafka producer")?;

        let mut kafka_error = false;
        let kafka_error_rx = futures::future::join(kafka_error_rx1, kafka_error_rx2);
        tokio::pin!(kafka_error_rx);

        // dedup
        let dedup = config.backend.create().await?;

        // input -> output loop
        let kafka_output = Arc::new(config.kafka_output);
        let mut send_tasks = JoinSet::new();
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break;
                }
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        _ = &mut kafka_error_rx => {
                            kafka_error = true;
                            break;
                        }
                        message = consumer.recv() => message,
                    }
                },
                message = consumer.recv() => message,
            }?;
            metrics::recv_inc();
            trace!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            let (key, payload) = match (
                message
                    .key()
                    .and_then(|k| String::from_utf8(k.to_vec()).ok()),
                message.payload(),
            ) {
                (Some(key), Some(payload)) => (key, payload.to_vec()),
                _ => continue,
            };
            let Some((slot, hash, bytes)) = key
                .split_once('_')
                .and_then(|(slot, hash)| slot.parse::<u64>().ok().map(|slot| (slot, hash)))
                .and_then(|(slot, hash)| {
                    let mut bytes: [u8; 32] = [0u8; 32];
                    const_hex::decode_to_slice(hash, &mut bytes)
                        .ok()
                        .map(|()| (slot, hash, bytes))
                })
            else {
                continue;
            };
            debug!("received message slot #{slot} with hash {hash}");

            let kafka = kafka.clone();
            let dedup = dedup.clone();
            let kafka_output = Arc::clone(&kafka_output);
            send_tasks.spawn(async move {
                if dedup.allowed(slot, bytes).await {
                    let record = FutureRecord::to(&kafka_output).key(&key).payload(&payload);
                    match kafka.send_result(record) {
                        Ok(future) => {
                            let result = future.await;
                            debug!("kafka send message with key: {key}, result: {result:?}");

                            result?.map_err(|(error, _message)| error)?;
                            metrics::sent_inc(GprcMessageKind::Unknown);
                            Ok::<(), anyhow::Error>(())
                        }
                        Err(error) => Err(error.0.into()),
                    }
                } else {
                    metrics::dedup_inc();
                    Ok(())
                }
            });
            if send_tasks.len() >= config.kafka_queue_size {
                tokio::select! {
                    _ = &mut shutdown => break,
                    _ = &mut kafka_error_rx => {
                        kafka_error = true;
                        break;
                    }
                    result = send_tasks.join_next() => {
                        if let Some(result) = result {
                            result??;
                        }
                    }
                }
            }
        }
        if !kafka_error {
            warn!("shutdown received...");
            loop {
                tokio::select! {
                    _ = &mut kafka_error_rx => break,
                    result = send_tasks.join_next() => match result {
                        Some(result) => result??,
                        None => break
                    }
                }
            }
        }
        Ok(())
    }

    async fn grpc2kafka(
        mut kafka_config: ClientConfig,
        config: ConfigGrpc2Kafka,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        // Connect to kafka
        let (kafka, kafka_error_rx) = metrics::StatsContext::create_future_producer(&kafka_config)
            .context("failed to create kafka producer")?;
        let mut kafka_error = false;
        tokio::pin!(kafka_error_rx);

        // Create gRPC client & subscribe
        let mut client = GeyserGrpcClient::build_from_shared(config.endpoint)?
            .x_token(config.x_token)?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(5))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;
        let mut geyser = client.subscribe_once(config.request.to_proto()).await?;

        // Receive-send loop
        let mut send_tasks = JoinSet::new();

        // Track latest slot for out-of-order detection
        let mut latest_slot_by_type: HashMap<String, u64> = HashMap::new();

        // Track throughput and queue statistics
        let mut message_count = 0u64;
        let mut last_throughput_update = metrics::get_current_timestamp_secs();
        let mut total_message_size = 0usize;
        let mut max_queue_depth = 0usize;

        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break;
                }
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        _ = &mut kafka_error_rx => {
                            kafka_error = true;
                            break;
                        }
                        message = geyser.next() => message,
                    }
                },
                message = geyser.next() => message,
            }
            .transpose()?;

            match message {
                Some(message) => {
                    // Record when we received the message
                    let receive_time = metrics::get_current_timestamp_secs();
                    message_count += 1;

                    let payload = message.encode_to_vec();
                    total_message_size += payload.len();

                    let message_inner = match &message.update_oneof {
                        Some(value) => value,
                        None => unreachable!("Expect valid message"),
                    };

                    // Record message size
                    metrics::record_message_size(payload.len());

                    // Handle ping/pong messages separately
                    match message_inner {
                        UpdateOneof::Ping(_) => {
                            tokio::spawn(ack_ping());
                            continue;
                        }
                        UpdateOneof::Pong(_) => {
                            tokio::spawn(ack_pong());
                            continue;
                        }
                        _ => {}
                    }

                    let Some(slot) = extract_slot_from_update(message_inner) else {
                        continue;
                    };

                    let hash = Sha256::digest(&payload);
                    let key = format!("{slot}_{}", const_hex::encode(hash));
                    let prom_kind = GprcMessageKind::from(message_inner);
                    let message_type = prom_kind.as_str();

                    // Track message throughput by type
                    metrics::inc_message_throughput_by_type(message_type, "inbound");

                    // Track slot timing and out-of-order detection
                    if let Some(latest_slot) = latest_slot_by_type.get(message_type) {
                        if slot < *latest_slot {
                            metrics::inc_out_of_order_slots(message_type);
                        }
                    }
                    latest_slot_by_type.insert(
                        message_type.to_string(),
                        slot.max(*latest_slot_by_type.get(message_type).unwrap_or(&0)),
                    );
                    metrics::set_latest_processed_slot("grpc2kafka", message_type, slot);

                    // Calculate latency from slot creation to receive time
                    if let Some(created_at) = &message.created_at {
                        let created_timestamp_secs =
                            created_at.seconds as f64 + (created_at.nanos as f64 / 1_000_000_000.0);
                        let slot_to_receive_latency = receive_time - created_timestamp_secs;

                        // Only record positive latencies (sometimes clocks can be off)
                        if slot_to_receive_latency > 0.0 {
                            metrics::record_slot_to_receive_latency(slot_to_receive_latency);
                            // Record by message type
                            metrics::record_message_latency_by_type(
                                message_type,
                                "slot_to_receive",
                                slot_to_receive_latency,
                            );
                        }

                        // Record slot timing drift (for debugging clock synchronization issues)
                        // This can be negative if our clock is ahead of the slot creation time
                        metrics::record_slot_timing_drift(slot_to_receive_latency);
                    }

                    let record = FutureRecord::to(&config.kafka_topic)
                        .key(&key)
                        .payload(&payload);

                    // Record message processing time (from receive to kafka send)
                    let kafka_send_time = metrics::get_current_timestamp_secs();
                    let processing_latency = kafka_send_time - receive_time;
                    metrics::record_message_processing_latency(processing_latency);
                    // Record by message type
                    metrics::record_message_latency_by_type(
                        message_type,
                        "processing",
                        processing_latency,
                    );

                    match kafka.send_result(record) {
                        Ok(future) => {
                            // Update queue depth and track high water mark
                            let current_queue_depth = send_tasks.len();
                            metrics::set_kafka_queue_depth("send", current_queue_depth as i64);
                            metrics::update_queue_depth_high_water_mark(
                                "send",
                                current_queue_depth as i64,
                            );

                            // Track max queue depth and memory usage
                            if current_queue_depth > max_queue_depth {
                                max_queue_depth = current_queue_depth;
                            }

                            // Estimate memory usage
                            let avg_message_size = if message_count > 0 {
                                total_message_size / message_count as usize
                            } else {
                                0
                            };
                            let estimated_memory = metrics::estimate_memory_usage(
                                current_queue_depth,
                                avg_message_size,
                            );
                            metrics::set_memory_usage_bytes(estimated_memory);

                            let created_at_for_task = message.created_at.clone();
                            let message_type_for_task = message_type.to_string();
                            let queue_enqueue_time = metrics::get_current_timestamp_secs();

                            let _ = send_tasks.spawn(async move {
                                // Calculate queue wait time
                                let task_start_time = metrics::get_current_timestamp_secs();
                                let queue_wait_latency = task_start_time - queue_enqueue_time;
                                metrics::record_queue_wait_time(queue_wait_latency);

                                let kafka_produce_start = metrics::get_current_timestamp_secs();
                                let result = future.await;
                                let kafka_produce_end = metrics::get_current_timestamp_secs();

                                debug!("kafka send message with key: {key}, result: {result:?}");

                                let _ = result?.map_err(|(error, _message)| error)?;

                                // Record Kafka produce latency
                                let produce_latency = kafka_produce_end - kafka_produce_start;
                                metrics::record_kafka_produce_latency(produce_latency);
                                // Record by message type
                                metrics::record_message_latency_by_type(
                                    &message_type_for_task,
                                    "kafka_produce",
                                    produce_latency,
                                );

                                // Record end-to-end latency if we have the created_at timestamp
                                if let Some(created_at) = created_at_for_task {
                                    let created_timestamp_secs = created_at.seconds as f64
                                        + (created_at.nanos as f64 / 1_000_000_000.0);
                                    let end_to_end_latency =
                                        kafka_produce_end - created_timestamp_secs;

                                    if end_to_end_latency > 0.0 {
                                        metrics::record_end_to_end_latency(end_to_end_latency);
                                        // Record by message type
                                        metrics::record_message_latency_by_type(
                                            &message_type_for_task,
                                            "end_to_end",
                                            end_to_end_latency,
                                        );
                                    }
                                }

                                // Track outbound throughput
                                metrics::inc_message_throughput_by_type(
                                    &message_type_for_task,
                                    "outbound",
                                );
                                metrics::sent_inc(prom_kind);
                                Ok::<(), anyhow::Error>(())
                            });

                            // Check for queue saturation
                            if send_tasks.len() >= config.kafka_queue_size {
                                metrics::inc_queue_saturation_events("kafka_send");

                                tokio::select! {
                                    _ = &mut shutdown => break,
                                    _ = &mut kafka_error_rx => {
                                        kafka_error = true;
                                        break;
                                    }
                                    result = send_tasks.join_next() => {
                                        if let Some(result) = result {
                                            result??;
                                        }
                                    }
                                }
                                // Update queue depth after processing a task
                                metrics::set_kafka_queue_depth("send", send_tasks.len() as i64);
                            }

                            // Periodically update throughput rates
                            let current_time = metrics::get_current_timestamp_secs();
                            if current_time - last_throughput_update >= 5.0 {
                                // Update every 5 seconds
                                let time_diff = current_time - last_throughput_update;
                                let message_rate = (message_count as f64 / time_diff) as i64;
                                metrics::update_message_rates(message_rate, message_rate);

                                // Reset counters
                                message_count = 0;
                                last_throughput_update = current_time;
                            }
                        }
                        Err(error) => return Err(error.0.into()),
                    }
                }
                None => break,
            }
        }
        if !kafka_error {
            warn!("shutdown received...");
            loop {
                tokio::select! {
                    _ = &mut kafka_error_rx => break,
                    result = send_tasks.join_next() => match result {
                        Some(result) => result??,
                        None => break
                    }
                }
            }
        }
        Ok(())
    }

    async fn kafka2grpc(
        mut kafka_config: ClientConfig,
        config: ConfigKafka2Grpc,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        for (key, value) in config.kafka.into_iter() {
            kafka_config.set(key, value);
        }

        let (grpc_tx, grpc_shutdown) = GrpcService::run(config.listen, config.channel_capacity)?;

        let (consumer, kafka_error_rx) =
            metrics::StatsContext::create_stream_consumer(&kafka_config)
                .context("failed to create kafka consumer")?;
        let mut kafka_error = false;
        tokio::pin!(kafka_error_rx);
        consumer.subscribe(&[&config.kafka_topic])?;

        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break
                },
                message = consumer.recv() => message?,
            };

            let kafka_receive_time = metrics::get_current_timestamp_secs();
            metrics::recv_inc();
            debug!(
                "received message with key: {:?}",
                message.key().and_then(|k| std::str::from_utf8(k).ok())
            );

            if let Some(payload) = message.payload() {
                // Record message size for consumer side
                metrics::record_message_size(payload.len());

                match SubscribeUpdate::decode(payload) {
                    Ok(decoded_message) => {
                        // Calculate consumer-side latency if created_at is available
                        if let Some(created_at) = &decoded_message.created_at {
                            let created_timestamp_secs = created_at.seconds as f64
                                + (created_at.nanos as f64 / 1_000_000_000.0);
                            let consumer_latency = kafka_receive_time - created_timestamp_secs;

                            if consumer_latency > 0.0 {
                                // This measures the time from when the message was originally created
                                // to when it was consumed from Kafka (including all processing time)
                                metrics::record_end_to_end_latency(consumer_latency);
                            }
                        }

                        // Track latest slot for consumer side
                        if let Some(update_oneof) = &decoded_message.update_oneof {
                            if let Some(slot) = extract_slot_from_update(update_oneof) {
                                let message_type = GprcMessageKind::from(update_oneof).as_str();
                                metrics::set_latest_processed_slot(
                                    "kafka2grpc",
                                    message_type,
                                    slot,
                                );

                                // Track consumer throughput by message type
                                metrics::inc_message_throughput_by_type(
                                    message_type,
                                    "kafka_consumer",
                                );

                                // Record consumer latency by message type
                                if let Some(created_at) = &decoded_message.created_at {
                                    let created_timestamp_secs = created_at.seconds as f64
                                        + (created_at.nanos as f64 / 1_000_000_000.0);
                                    let consumer_latency =
                                        kafka_receive_time - created_timestamp_secs;

                                    if consumer_latency > 0.0 {
                                        metrics::record_message_latency_by_type(
                                            message_type,
                                            "kafka_consumer",
                                            consumer_latency,
                                        );
                                    }
                                }
                            }
                        }

                        let grpc_send_result = grpc_tx.send(decoded_message);
                        match grpc_send_result {
                            Ok(_) => {
                                // Message successfully sent to gRPC subscribers
                            }
                            Err(_) => {
                                // gRPC channel is closed or full
                                debug!("Failed to send message to gRPC subscribers - channel closed or full");
                            }
                        }
                    }
                    Err(error) => {
                        warn!("failed to decode message: {error}");
                    }
                }
            }
        }

        if !kafka_error {
            warn!("shutdown received...");
        }
        Ok(grpc_shutdown.await??)
    }

    async fn wss2kafka(
        kafka_config: ClientConfig,
        config: ConfigWss2Kafka,
        mut shutdown: BoxFuture<'static, ()>,
    ) -> anyhow::Result<()> {
        // Connect to kafka
        let (kafka, kafka_error_rx) = metrics::StatsContext::create_future_producer(&kafka_config)
            .context("failed to create kafka producer")?;
        let mut kafka_error = false;
        tokio::pin!(kafka_error_rx);

        // Connect to websocket
        let (ws_stream, _response) = connect_async(&config.endpoint).await?;
        let (mut write_sink, mut read_stream) = ws_stream.split();

        // Subscribe to events
        write_sink.send(TokioMessage::text(config.request)).await?;

        // Receive-send loop
        let mut send_tasks = JoinSet::new();
        loop {
            let message = tokio::select! {
                _ = &mut shutdown => break,
                _ = &mut kafka_error_rx => {
                    kafka_error = true;
                    break;
                }
                maybe_result = send_tasks.join_next() => match maybe_result {
                    Some(result) => {
                        result??;
                        continue;
                    }
                    None => tokio::select! {
                        _ = &mut shutdown => break,
                        _ = &mut kafka_error_rx => {
                            kafka_error = true;
                            break;
                        }
                        message = read_stream.next() => message,
                    }
                },
                message = read_stream.next() => message,
            }
            .transpose()?;

            match message {
                Some(message) => {
                    if message.is_close() {
                        break;
                    }
                    if message.is_ping() {
                        write_sink.send(TokioMessage::Pong(message.into())).await?;
                        tokio::spawn(ack_ping());
                        continue;
                    }
                    if message.is_pong() {
                        tokio::spawn(ack_pong());
                        continue;
                    }
                    if !message.is_text() {
                        continue;
                    }
                    let payload = WssEvent::new(message.to_string(), &config.provider).to_string();
                    let hash = Sha256::digest(&payload);
                    let key = const_hex::encode(hash);

                    let record = FutureRecord::to(&config.kafka_topic)
                        .key(&key)
                        .payload(&payload);

                    match kafka.send_result(record) {
                        Ok(future) => {
                            let _ = send_tasks.spawn(async move {
                                let result = future.await;
                                debug!("kafka send message with key: {key}, result: {result:?}");

                                let _ = result?.map_err(|(error, _message)| error)?;
                                metrics::sent_inc(GprcMessageKind::Unknown);
                                Ok::<(), anyhow::Error>(())
                            });
                            if send_tasks.len() >= config.kafka_queue_size {
                                tokio::select! {
                                    _ = &mut shutdown => break,
                                    _ = &mut kafka_error_rx => {
                                        kafka_error = true;
                                        break;
                                    }
                                    result = send_tasks.join_next() => {
                                        if let Some(result) = result {
                                            result??;
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => return Err(error.0.into()),
                    }
                }
                None => break,
            }
        }
        if !kafka_error {
            warn!("shutdown received...");
            loop {
                tokio::select! {
                    _ = &mut kafka_error_rx => break,
                    result = send_tasks.join_next() => match result {
                        Some(result) => result??,
                        None => break
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;

    // Parse args
    let args = Args::parse();
    let config = args.action.load_config()?;

    // Run prometheus server
    if let Some(address) = args.prometheus.or(config.prometheus) {
        prometheus_run_server(address).await?;
    }

    // Create kafka config
    let mut kafka_config = ClientConfig::new();
    for (key, value) in config.kafka.iter() {
        kafka_config.set(key, value);
    }

    args.action.run(config, kafka_config).await
}
