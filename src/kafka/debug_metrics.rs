use {
    crate::{
        kafka::metrics,
        metrics::GprcMessageKind,
    },
    std::collections::HashMap,
    yellowstone_grpc_proto::{
        prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
        prost::Message,
        prost_types::Timestamp,
    },
};

/// Debug metrics tracker for gRPC to Kafka pipeline
/// Handles all latency measurements, throughput calculations, and performance monitoring
#[derive(Debug)]
pub struct DebugMetricsTracker {
    /// Track latest slot by message type for out-of-order detection
    latest_slot_by_type: HashMap<String, u64>,
    
    /// Message count for throughput calculations
    message_count: u64,
    
    /// Last time throughput was updated
    last_throughput_update: f64,
    
    /// Total message size for memory estimation
    total_message_size: usize,
    
    /// Maximum queue depth reached
    max_queue_depth: usize,
}

impl DebugMetricsTracker {
    /// Create a new debug metrics tracker
    pub fn new() -> Self {
        Self {
            latest_slot_by_type: HashMap::new(),
            message_count: 0,
            last_throughput_update: metrics::get_current_timestamp_secs(),
            total_message_size: 0,
            max_queue_depth: 0,
        }
    }

    /// Process a received message and record all relevant metrics
    pub fn record_message_received(&mut self, message: &SubscribeUpdate, receive_time: f64) {
        self.message_count += 1;
        
        let payload_size = message.encoded_len();
        self.total_message_size += payload_size;
        
        // Record message size
        metrics::record_message_size(payload_size);
        
        // Process slot-related metrics if we have an update
        if let Some(update_oneof) = &message.update_oneof {
            if let Some(slot) = extract_slot_from_update(update_oneof) {
                let message_type = GprcMessageKind::from(update_oneof).as_str();
                
                // Track inbound throughput by type
                metrics::inc_message_throughput_by_type(message_type, "inbound");
                
                // Track out-of-order detection
                self.track_slot_ordering(slot, message_type);
                
                // Record slot-to-receive latency if created_at is available
                self.record_slot_latency(message, receive_time, message_type);
            }
        }
    }

    /// Track slot ordering and detect out-of-order messages
    fn track_slot_ordering(&mut self, slot: u64, message_type: &str) {
        if let Some(latest_slot) = self.latest_slot_by_type.get(message_type) {
            if slot < *latest_slot {
                metrics::inc_out_of_order_slots(message_type);
            }
        }
        
        let current_latest = *self.latest_slot_by_type.get(message_type).unwrap_or(&0);
        self.latest_slot_by_type.insert(
            message_type.to_string(),
            slot.max(current_latest),
        );
        
        metrics::set_latest_processed_slot("grpc2kafka", message_type, slot);
    }

    /// Record latency from slot creation to message receipt
    fn record_slot_latency(&self, message: &SubscribeUpdate, receive_time: f64, message_type: &str) {
        if let Some(created_at) = &message.created_at {
            let created_timestamp_secs =
                created_at.seconds as f64 + (created_at.nanos as f64 / 1_000_000_000.0);
            let slot_to_receive_latency = receive_time - created_timestamp_secs;

            // Only record positive latencies (sometimes clocks can be off)
            if slot_to_receive_latency > 0.0 {
                metrics::record_slot_to_receive_latency(slot_to_receive_latency);
                metrics::record_message_latency_by_type(
                    message_type,
                    "slot_to_receive",
                    slot_to_receive_latency,
                );
            }

            // Record slot timing drift (for debugging clock synchronization issues)
            metrics::record_slot_timing_drift(slot_to_receive_latency);
        }
    }

    /// Record processing latency (from receive to kafka send)
    pub fn record_processing_latency(&self, receive_time: f64, message_type: &str) {
        let kafka_send_time = metrics::get_current_timestamp_secs();
        let processing_latency = kafka_send_time - receive_time;
        
        metrics::record_message_processing_latency(processing_latency);
        metrics::record_message_latency_by_type(
            message_type,
            "processing",
            processing_latency,
        );
    }

    /// Update queue depth metrics and memory usage estimates
    pub fn update_queue_metrics(&mut self, current_queue_depth: usize) {
        // Update queue depth metrics
        metrics::set_kafka_queue_depth("send", current_queue_depth as i64);
        metrics::update_queue_depth_high_water_mark("send", current_queue_depth as i64);
        
        // Track max queue depth
        if current_queue_depth > self.max_queue_depth {
            self.max_queue_depth = current_queue_depth;
        }
        
        // Estimate memory usage
        let avg_message_size = if self.message_count > 0 {
            self.total_message_size / self.message_count as usize
        } else {
            0
        };
        let estimated_memory = metrics::estimate_memory_usage(current_queue_depth, avg_message_size);
        metrics::set_memory_usage_bytes(estimated_memory);
    }

    /// Record queue saturation event
    pub fn record_queue_saturation(&self) {
        metrics::inc_queue_saturation_events("kafka_send");
    }

    /// Update throughput rates periodically
    pub fn update_throughput_rates(&mut self) {
        let current_time = metrics::get_current_timestamp_secs();
        
        // Update every 5 seconds
        if current_time - self.last_throughput_update >= 5.0 {
            let time_diff = current_time - self.last_throughput_update;
            let message_rate = (self.message_count as f64 / time_diff) as i64;
            metrics::update_message_rates(message_rate, message_rate);
            
            // Reset counters
            self.message_count = 0;
            self.last_throughput_update = current_time;
        }
    }

    /// Final update of queue depth after task completion
    pub fn update_queue_depth_after_task(&self, queue_depth: usize) {
        metrics::set_kafka_queue_depth("send", queue_depth as i64);
    }
}

/// Parameters for Kafka send task metrics
pub struct KafkaSendTaskMetrics {
    pub message_type: String,
    pub created_at: Option<Timestamp>,
    pub queue_enqueue_time: f64,
    pub prom_kind: GprcMessageKind,
}

impl KafkaSendTaskMetrics {
    pub fn new(message: &SubscribeUpdate, prom_kind: GprcMessageKind) -> Self {
        let message_type = if let Some(update_oneof) = &message.update_oneof {
            GprcMessageKind::from(update_oneof).as_str().to_string()
        } else {
            "unknown".to_string()
        };

        Self {
            message_type,
            created_at: message.created_at.clone(),
            queue_enqueue_time: metrics::get_current_timestamp_secs(),
            prom_kind,
        }
    }

    /// Record all metrics for a completed Kafka send task
    pub fn record_task_completion(&self) -> anyhow::Result<()> {
        // Calculate queue wait time
        let task_start_time = metrics::get_current_timestamp_secs();
        let queue_wait_latency = task_start_time - self.queue_enqueue_time;
        metrics::record_queue_wait_time(queue_wait_latency);

        Ok(())
    }

    /// Record Kafka produce latency and end-to-end metrics
    pub fn record_kafka_produce_metrics(&self, produce_start: f64, produce_end: f64) {
        let produce_latency = produce_end - produce_start;
        
        // Record Kafka produce latency
        metrics::record_kafka_produce_latency(produce_latency);
        metrics::record_message_latency_by_type(
            &self.message_type,
            "kafka_produce",
            produce_latency,
        );

        // Record end-to-end latency if we have the created_at timestamp
        if let Some(created_at) = &self.created_at {
            let created_timestamp_secs =
                created_at.seconds as f64 + (created_at.nanos as f64 / 1_000_000_000.0);
            let end_to_end_latency = produce_end - created_timestamp_secs;

            if end_to_end_latency > 0.0 {
                metrics::record_end_to_end_latency(end_to_end_latency);
                metrics::record_message_latency_by_type(
                    &self.message_type,
                    "end_to_end",
                    end_to_end_latency,
                );
            }
        }

        // Track outbound throughput
        metrics::inc_message_throughput_by_type(&self.message_type, "outbound");
        metrics::sent_inc(self.prom_kind);
    }
}

/// Helper function to extract slot from UpdateOneof (moved from main file)
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