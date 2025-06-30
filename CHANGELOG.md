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

## [4.0.1] - 2025-06-27

### Fixes

- Duplication error involving sending message with the created_at field which make the sha256 be different,
resulting on a different update even if its the same.

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
