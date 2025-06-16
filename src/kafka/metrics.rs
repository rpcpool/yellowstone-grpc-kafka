use {
    crate::metrics::GprcMessageKind,
    prometheus::{GaugeVec, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGaugeVec, Opts},
    rdkafka::{
        client::{ClientContext, DefaultClientContext},
        config::{ClientConfig, FromClientConfigAndContext, RDKafkaLogLevel},
        consumer::{ConsumerContext, StreamConsumer},
        error::{KafkaError, KafkaResult},
        producer::FutureProducer,
        statistics::Statistics,
    },
    std::{sync::Mutex, time::{SystemTime, UNIX_EPOCH}},
    tokio::sync::oneshot,
};

lazy_static::lazy_static! {
    pub(crate) static ref KAFKA_STATS: GaugeVec = GaugeVec::new(
        Opts::new("kafka_stats", "librdkafka metrics"),
        &["broker", "metric"]
    ).unwrap();

    pub(crate) static ref KAFKA_DEDUP_TOTAL: IntCounter = IntCounter::new(
        "kafka_dedup_total", "Total number of deduplicated messages"
    ).unwrap();

    pub(crate) static ref KAFKA_RECV_TOTAL: IntCounter = IntCounter::new(
        "kafka_recv_total", "Total number of received messages"
    ).unwrap();

    pub(crate) static ref KAFKA_SENT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("kafka_sent_total", "Total number of uploaded messages by type"),
        &["kind"]
    ).unwrap();

    // New latency debugging metrics
    pub(crate) static ref MESSAGE_PROCESSING_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "message_processing_latency_seconds",
            "Time from gRPC message receive to Kafka send"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    pub(crate) static ref KAFKA_PRODUCE_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "kafka_produce_latency_seconds", 
            "Time from Kafka send to acknowledgment"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    pub(crate) static ref SLOT_TO_RECEIVE_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "slot_to_receive_latency_seconds",
            "Time from slot creation to message receipt (based on created_at timestamp)"
        ).buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0])
    ).unwrap();

    pub(crate) static ref END_TO_END_LATENCY: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "end_to_end_latency_seconds",
            "Total time from slot creation to Kafka acknowledgment"
        ).buckets(vec![0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0])
    ).unwrap();

    pub(crate) static ref KAFKA_QUEUE_DEPTH: IntGaugeVec = IntGaugeVec::new(
        Opts::new("kafka_queue_depth", "Number of pending Kafka operations"),
        &["operation_type"]
    ).unwrap();

    pub(crate) static ref SLOT_TIMING_DRIFT: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "slot_timing_drift_seconds",
            "Difference between expected slot time and actual receive time"
        ).buckets(vec![-300.0, -60.0, -30.0, -10.0, -5.0, -1.0, 0.0, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0])
    ).unwrap();

    pub(crate) static ref MESSAGE_SIZE_BYTES: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "message_size_bytes",
            "Size of messages being processed"
        ).buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0, 500000.0, 1000000.0])
    ).unwrap();

    pub(crate) static ref OUT_OF_ORDER_SLOTS: IntCounterVec = IntCounterVec::new(
        Opts::new("out_of_order_slots_total", "Number of slots received out of order"),
        &["message_type"]
    ).unwrap();

    pub(crate) static ref LATEST_PROCESSED_SLOT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("latest_processed_slot", "Latest slot number processed by component"),
        &["component", "message_type"]
    ).unwrap();
}

#[derive(Debug)]
pub struct StatsContext {
    default: DefaultClientContext,
    error_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl StatsContext {
    fn new() -> (Self, oneshot::Receiver<()>) {
        let (error_tx, error_rx) = oneshot::channel();
        (
            Self {
                default: DefaultClientContext,
                error_tx: Mutex::new(Some(error_tx)),
            },
            error_rx,
        )
    }

    fn send_error(&self) {
        if let Some(error_tx) = self.error_tx.lock().expect("alive mutex").take() {
            let _ = error_tx.send(());
        }
    }
}

impl ClientContext for StatsContext {
    fn stats(&self, statistics: Statistics) {
        for (name, broker) in statistics.brokers {
            macro_rules! set_value {
                ($name:expr, $value:expr) => {
                    KAFKA_STATS
                        .with_label_values(&[&name, $name])
                        .set($value as f64);
                };
            }

            set_value!("outbuf_cnt", broker.outbuf_cnt);
            set_value!("outbuf_msg_cnt", broker.outbuf_msg_cnt);
            set_value!("waitresp_cnt", broker.waitresp_cnt);
            set_value!("waitresp_msg_cnt", broker.waitresp_msg_cnt);
            set_value!("tx", broker.tx);
            set_value!("txerrs", broker.txerrs);
            set_value!("txretries", broker.txretries);
            set_value!("req_timeouts", broker.req_timeouts);

            if let Some(window) = broker.int_latency {
                set_value!("int_latency.min", window.min);
                set_value!("int_latency.max", window.max);
                set_value!("int_latency.avg", window.avg);
                set_value!("int_latency.sum", window.sum);
                set_value!("int_latency.cnt", window.cnt);
                set_value!("int_latency.stddev", window.stddev);
                set_value!("int_latency.hdrsize", window.hdrsize);
                set_value!("int_latency.p50", window.p50);
                set_value!("int_latency.p75", window.p75);
                set_value!("int_latency.p90", window.p90);
                set_value!("int_latency.p95", window.p95);
                set_value!("int_latency.p99", window.p99);
                set_value!("int_latency.p99_99", window.p99_99);
                set_value!("int_latency.outofrange", window.outofrange);
            }

            if let Some(window) = broker.outbuf_latency {
                set_value!("outbuf_latency.min", window.min);
                set_value!("outbuf_latency.max", window.max);
                set_value!("outbuf_latency.avg", window.avg);
                set_value!("outbuf_latency.sum", window.sum);
                set_value!("outbuf_latency.cnt", window.cnt);
                set_value!("outbuf_latency.stddev", window.stddev);
                set_value!("outbuf_latency.hdrsize", window.hdrsize);
                set_value!("outbuf_latency.p50", window.p50);
                set_value!("outbuf_latency.p75", window.p75);
                set_value!("outbuf_latency.p90", window.p90);
                set_value!("outbuf_latency.p95", window.p95);
                set_value!("outbuf_latency.p99", window.p99);
                set_value!("outbuf_latency.p99_99", window.p99_99);
                set_value!("outbuf_latency.outofrange", window.outofrange);
            }
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.default.log(level, fac, log_message);
        if matches!(
            level,
            RDKafkaLogLevel::Emerg
                | RDKafkaLogLevel::Alert
                | RDKafkaLogLevel::Critical
                | RDKafkaLogLevel::Error
        ) {
            self.send_error()
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.default.error(error, reason);
        self.send_error()
    }
}

impl ConsumerContext for StatsContext {}

impl StatsContext {
    pub fn create_future_producer(
        config: &ClientConfig,
    ) -> KafkaResult<(FutureProducer<Self>, oneshot::Receiver<()>)> {
        let (context, error_rx) = Self::new();
        FutureProducer::from_config_and_context(config, context)
            .map(|producer| (producer, error_rx))
    }

    pub fn create_stream_consumer(
        config: &ClientConfig,
    ) -> KafkaResult<(StreamConsumer<Self>, oneshot::Receiver<()>)> {
        let (context, error_rx) = Self::new();
        StreamConsumer::from_config_and_context(config, context)
            .map(|consumer| (consumer, error_rx))
    }
}

pub fn dedup_inc() {
    KAFKA_DEDUP_TOTAL.inc();
}

pub fn recv_inc() {
    KAFKA_RECV_TOTAL.inc();
}

pub fn sent_inc(kind: GprcMessageKind) {
    KAFKA_SENT_TOTAL.with_label_values(&[kind.as_str()]).inc()
}

// New metric helper functions
pub fn record_message_processing_latency(seconds: f64) {
    MESSAGE_PROCESSING_LATENCY.observe(seconds);
}

pub fn record_kafka_produce_latency(seconds: f64) {
    KAFKA_PRODUCE_LATENCY.observe(seconds);
}

pub fn record_slot_to_receive_latency(seconds: f64) {
    SLOT_TO_RECEIVE_LATENCY.observe(seconds);
}

pub fn record_end_to_end_latency(seconds: f64) {
    END_TO_END_LATENCY.observe(seconds);
}

pub fn set_kafka_queue_depth(operation_type: &str, depth: i64) {
    KAFKA_QUEUE_DEPTH.with_label_values(&[operation_type]).set(depth);
}

pub fn record_slot_timing_drift(seconds: f64) {
    SLOT_TIMING_DRIFT.observe(seconds);
}

pub fn record_message_size(bytes: usize) {
    MESSAGE_SIZE_BYTES.observe(bytes as f64);
}

pub fn inc_out_of_order_slots(message_type: &str) {
    OUT_OF_ORDER_SLOTS.with_label_values(&[message_type]).inc();
}

pub fn set_latest_processed_slot(component: &str, message_type: &str, slot: u64) {
    LATEST_PROCESSED_SLOT.with_label_values(&[component, message_type]).set(slot as i64);
}

pub fn get_current_timestamp_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
