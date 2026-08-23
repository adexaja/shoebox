# Changelog

All notable changes to Shoebox are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and releases follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Release automation for server binaries and the container image.

## [0.1.6] - 2026-08-18

### Added

- Configurable PostgreSQL schema selection.

## [0.1.5] - 2026-08-18

### Added

- Configurable in-memory deduplication policies, including bounded LRU mode.

## [0.1.4] - 2026-08-14

### Fixed

- Serialized PostgreSQL schema initialization across concurrent openers.

## [0.1.3] - 2026-08-12

### Fixed

- Queue draining of scheduled retries during shutdown.

## [0.1.2] - 2026-08-12

### Fixed

- Queue lifecycle handling and persistent dead-letter queue behavior.

## [0.1.1] - 2026-08-11

### Added

- PostgreSQL benchmark coverage.

## [0.1.0] - 2026-08-11

### Fixed

- Context handling while reading messages.

[Unreleased]: https://github.com/adexaja/shoebox/compare/v0.1.6...HEAD
[0.1.6]: https://github.com/adexaja/shoebox/releases/tag/v0.1.6
[0.1.5]: https://github.com/adexaja/shoebox/releases/tag/v0.1.5
[0.1.4]: https://github.com/adexaja/shoebox/releases/tag/v0.1.4
[0.1.3]: https://github.com/adexaja/shoebox/releases/tag/v0.1.3
[0.1.2]: https://github.com/adexaja/shoebox/releases/tag/v0.1.2
[0.1.1]: https://github.com/adexaja/shoebox/releases/tag/v0.1.1
[0.1.0]: https://github.com/adexaja/shoebox/releases/tag/v0.1.0
