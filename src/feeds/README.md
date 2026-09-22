# Exchange feeds

Each adapter discovers the venue's symbol format, subscribes to its public quote/funding channels, converts payloads into the shared `MarketEvent` model, and reconnects with bounded backoff.

The collector and UI consume the same normalized feed events. Adapters remain read-only and do not contain order-placement paths.
