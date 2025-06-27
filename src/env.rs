use {
    crate::{
        config::ConfigGrpcRequest,
        kafka::config::{Config, ConfigGrpc2Kafka, ConfigWss2Kafka},
    },
    anyhow::Context,
    serde::de,
    std::{collections::HashMap, env, net::SocketAddr},
};

pub fn load_grpc2kafka_config() -> anyhow::Result<Config> {
    let config = Config {
        prometheus: build_prometheus_address()?,
        kafka: build_kafka()?,
        dedup: None,
        grpc2kafka: Some(build_grpc2kafka()?),
        kafka2grpc: None,
        wss2kafka: None,
    };
    Ok(config)
}

pub fn load_wss2kafka_config() -> anyhow::Result<Config> {
    let config = Config {
        prometheus: build_prometheus_address()?,
        kafka: build_kafka()?,
        dedup: None,
        grpc2kafka: None,
        kafka2grpc: None,
        wss2kafka: Some(build_wss2kafka()?),
    };
    Ok(config)
}

fn build_prometheus_address() -> anyhow::Result<Option<SocketAddr>> {
    let prometheus_address = optional_env_value("PROMETHEUS_ADDRESS");

    if let Some(string_address) = prometheus_address {
        let socket_address: SocketAddr = string_address.parse()?;
        Ok(Some(socket_address))
    } else {
        Ok(None)
    }
}

fn build_kafka() -> anyhow::Result<HashMap<String, String>> {
    let mut kafka: HashMap<String, String> = HashMap::new();

    let required_configs = [
        ("bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS"),
        ("compression.codec", "KAFKA_COMPRESSION_CODEC"),
        ("statistics.interval.ms", "KAFKA_STATISTICS_INTERVAL_MS"),
    ];

    let optional_configs = [
        ("security.protocol", "KAFKA_SECURITY_PROTOCOL"),
        ("sasl.mechanisms", "KAFKA_SASL_MECHANISMS"),
        ("sasl.username", "KAFKA_SASL_USERNAME"),
        ("sasl.password", "KAFKA_SASL_PASSWORD"),
        ("broker.address.family", "KAFKA_BROKER_ADDRESS_FAMILY"),
    ];

    for (config_name, env_name) in required_configs {
        let value = required_env_value(env_name)?;
        kafka.insert(config_name.to_string(), value);
    }

    for (config_name, env_name) in optional_configs {
        if let Some(value) = optional_env_value(env_name) {
            kafka.insert(config_name.to_string(), value);
        }
    }

    Ok(kafka)
}

fn build_grpc2kafka() -> anyhow::Result<ConfigGrpc2Kafka> {
    Ok(ConfigGrpc2Kafka {
        endpoint: required_env_value("REQUEST_ENDPOINT")?,
        x_token: Some(required_secret("REQUEST_X_TOKEN")?),
        request: deserialize_request::<ConfigGrpcRequest>(required_env_value("REQUEST_BODY")?)?,
        kafka: HashMap::new(),
        kafka_topic: required_env_value("KAFKA_TOPIC")?,
        kafka_queue_size: required_env_value("KAFKA_QUEUE_SIZE")?.parse()?,
    })
}

fn build_wss2kafka() -> anyhow::Result<ConfigWss2Kafka> {
    Ok(ConfigWss2Kafka {
        endpoint: required_secret("REQUEST_ENDPOINT")?,
        x_token: None,
        request: required_env_value("REQUEST_BODY")?,
        kafka: HashMap::new(),
        kafka_topic: required_env_value("KAFKA_TOPIC")?,
        kafka_queue_size: required_env_value("KAFKA_QUEUE_SIZE")?.parse()?,
        provider: required_env_value("REQUEST_PROVIDER")?.parse()?,
    })
}

fn deserialize_request<T>(json_string: String) -> anyhow::Result<T>
where
    T: de::DeserializeOwned,
{
    json5::from_str(&json_string).context(format!("failed to parse request {json_string:?}"))
}

fn required_env_value(name: &str) -> anyhow::Result<String> {
    env::var(name).context(format!("failed to load env var {name:?}"))
}

fn optional_env_value(name: &str) -> Option<String> {
    env::var(name).ok()
}

fn required_secret(name: &str) -> anyhow::Result<String> {
    let key = required_env_value(name)?;
    required_env_value(&key)
}
