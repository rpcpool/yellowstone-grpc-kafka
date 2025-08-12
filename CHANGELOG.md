# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Fixes

### Features

### Breaking

## [4.1.0] - 2025-08-12

### Features

- Bump yellowstone-grpc-client/proto to v8.0.0 to use Solana v2.3.

## [4.0.1] - 2025-06-27

### Fixes

- Fix deduplication failure when using multiple gRPC sources. Previously hashed the entire 
  SubscribeUpdate including variable metadata (created_at, filters), causing identical 
  messages from different sources to generate different hashes. Now hashes only the 
  inner update_oneof content for deduplication while preserving the full SubscribeUpdate 
  message in Kafka, ensuring both consistent deduplication across sources and proper 
  message format for consumers.
- Fix kafka2grpc decode errors that occurred with initial dedup fix. The initial fix 
  incorrectly sent only the inner message content to Kafka, breaking compatibility 
  with consumers expecting full SubscribeUpdate messages.
- Bump tokio to 1.45.1 (security advisory)
- Bump openssl to 0.10.73 (security advisory)

## [4.0.0] - 2025-03-10

### Features

- solana: upgrade to v2.2 ([#4](https://github.com/rpcpool/yellowstone-grpc-kafka/pull/4))

## [3.0.0] - 2024-11-22

### Features

- solana: upgrade to v2.1 ([#2](https://github.com/rpcpool/yellowstone-grpc-kafka/pull/2))

## [2.0.1] - 2024-10-21

### Fixes

- use TlsConfig with native roots ([#1](https://github.com/rpcpool/yellowstone-grpc-kafka/pull/1))

## [2.0.0] - 2024-10-04

- Bump solana to to `2.0`.

## [1.0.0] - 2024-10-03

- Init. Moved from https://github.com/rpcpool/yellowstone-grpc.
