use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use eframe::egui::{self, Color32};

use crate::config::AppConfig;
use crate::model::{ArbRow, Exchange, ExchangeFeedHealth, now_ms};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortColumn {
    Symbol,
    BuyExchange,
    SellExchange,
    Funding,
    BuyAsk,
    SellBid,
    RawSpread,
    NetSpread,
    MaxBaseQty,
    MaxUsdNotional,
    AgeMs,
    LatencyMs,
}

pub struct ArbApp {
    snapshot: Arc<RwLock<Vec<ArbRow>>>,
    exchange_health: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
    frame_delay: Duration,
    exchange_timeout_ms: i64,
    sort_column: SortColumn,
    sort_descending: bool,
}

const POSITIVE_COLOR: Color32 = Color32::from_rgb(70, 170, 95);
const NEGATIVE_COLOR: Color32 = Color32::from_rgb(205, 75, 75);
const STALE_COLOR: Color32 = Color32::from_rgb(215, 160, 65);
const AGE_OK_MS: i64 = 1_200;
const LATENCY_OK_MS: i64 = 300;
const DEFAULT_EXCHANGE_TIMEOUT_MS: i64 = 15_000;

impl ArbApp {
    pub fn new(
        snapshot: Arc<RwLock<Vec<ArbRow>>>,
        exchange_health: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
        config: AppConfig,
    ) -> Self {
        let fps = config.ui_fps.max(1);
        Self {
            snapshot,
            exchange_health,
            frame_delay: Duration::from_millis((1000 / fps) as u64),
            exchange_timeout_ms: config.stale_ms.max(DEFAULT_EXCHANGE_TIMEOUT_MS),
            sort_column: SortColumn::NetSpread,
            sort_descending: true,
        }
    }

    fn read_snapshot(&self) -> Vec<ArbRow> {
        match self.snapshot.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn read_exchange_health(&self) -> HashMap<Exchange, ExchangeFeedHealth> {
        match self.exchange_health.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn toggle_sort(&mut self, requested: SortColumn) {
        if self.sort_column == requested {
            self.sort_descending = !self.sort_descending;
        } else {
            self.sort_column = requested;
            self.sort_descending = true;
        }
    }

    fn sort_rows(&self, rows: &mut [ArbRow]) {
        rows.sort_by(|left, right| {
            let order = match self.sort_column {
                SortColumn::Symbol => left.symbol.cmp(&right.symbol),
                SortColumn::BuyExchange => left.buy_ex.as_str().cmp(right.buy_ex.as_str()),
                SortColumn::SellExchange => left.sell_ex.as_str().cmp(right.sell_ex.as_str()),
                SortColumn::Funding => cmp_f64(funding_sort_score(left), funding_sort_score(right))
                    .then_with(|| {
                        cmp_f64(
                            left.buy_funding_rate.unwrap_or(f64::NEG_INFINITY),
                            right.buy_funding_rate.unwrap_or(f64::NEG_INFINITY),
                        )
                    }),
                SortColumn::BuyAsk => cmp_f64(left.buy_ask, right.buy_ask),
                SortColumn::SellBid => cmp_f64(left.sell_bid, right.sell_bid),
                SortColumn::RawSpread => cmp_f64(left.raw_spread_bps, right.raw_spread_bps),
                SortColumn::NetSpread => cmp_f64(left.net_spread_bps, right.net_spread_bps),
                SortColumn::MaxBaseQty => cmp_f64(left.max_base_qty, right.max_base_qty),
                SortColumn::MaxUsdNotional => {
                    cmp_f64(left.max_usd_notional, right.max_usd_notional)
                }
                SortColumn::AgeMs => left.age_ms.cmp(&right.age_ms),
                SortColumn::LatencyMs => left.latency_ms.cmp(&right.latency_ms),
            };

            if self.sort_descending {
                order.reverse()
            } else {
                order
            }
        });
    }

    fn header_button(&mut self, ui: &mut egui::Ui, label: &str, column: SortColumn) {
        let mut button_label = label.to_owned();
        if self.sort_column == column {
            button_label.push(' ');
            button_label.push_str(if self.sort_descending { "▼" } else { "▲" });
        }

        if ui.button(button_label).clicked() {
            self.toggle_sort(column);
        }
    }

    fn row_cell_exchange(ui: &mut egui::Ui, exchange: Exchange, is_buy_leg: bool) {
        let color = if is_buy_leg {
            POSITIVE_COLOR
        } else {
            NEGATIVE_COLOR
        };
        ui.colored_label(color, exchange.as_str());
    }

    fn row_cell_funding(ui: &mut egui::Ui, row: &ArbRow) {
        let buy = row.buy_funding_rate.map(|rate| rate * 100.0);
        let sell = row.sell_funding_rate.map(|rate| rate * 100.0);

        ui.horizontal(|ui| {
            row_cell_funding_side(ui, "B", buy, row.buy_funding_stale);
            ui.label("|");
            row_cell_funding_side(ui, "S", sell, row.sell_funding_stale);
        });
    }
}

fn cmp_f64(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or(Ordering::Equal)
}

fn funding_sort_score(row: &ArbRow) -> f64 {
    match (row.buy_funding_rate, row.sell_funding_rate) {
        (Some(buy), Some(sell)) => (buy - sell).abs(),
        (Some(buy), None) => buy.abs(),
        (None, Some(sell)) => sell.abs(),
        (None, None) => f64::NEG_INFINITY,
    }
}

fn spread_color(spread_bps: f64) -> Color32 {
    if spread_bps >= 0.0 {
        POSITIVE_COLOR
    } else {
        NEGATIVE_COLOR
    }
}

fn format_price_compact(value: f64) -> String {
    let mut text = format!("{value:.10}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn row_cell_funding_side(ui: &mut egui::Ui, label: &str, rate_pct: Option<f64>, is_stale: bool) {
    match rate_pct {
        Some(rate) => {
            let value_label = if is_stale {
                format!("{label} {:+.4}% (stale)", rate)
            } else {
                format!("{label} {:+.4}%", rate)
            };
            let color = if is_stale {
                STALE_COLOR
            } else {
                spread_color(rate)
            };
            ui.colored_label(color, value_label);
        }
        None => {
            ui.label(format!("{label} n/a"));
        }
    }
}

fn age_color(age_ms: i64) -> Color32 {
    if age_ms <= AGE_OK_MS {
        POSITIVE_COLOR
    } else {
        NEGATIVE_COLOR
    }
}

fn latency_color(latency_ms: i64) -> Color32 {
    if latency_ms <= LATENCY_OK_MS {
        POSITIVE_COLOR
    } else {
        NEGATIVE_COLOR
    }
}

impl eframe::App for ArbApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.request_repaint_after(self.frame_delay);

        let mut rows = self.read_snapshot();
        self.sort_rows(&mut rows);

        egui::TopBottomPanel::top("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Cross-Exchange Arb Scanner");
                ui.separator();
                ui.label(format!("Rows: {}", rows.len()));
            });

            ui.add_space(4.0);
            ui.horizontal_wrapped(|ui| {
                let now_ms = now_ms();
                let health_map = self.read_exchange_health();
                for exchange in Exchange::all() {
                    let health = health_map.get(exchange);
                    let last_event_ms = health.and_then(|item| item.last_event_ms);
                    let is_active = last_event_ms
                        .map(|last_seen| (now_ms - last_seen).max(0) <= self.exchange_timeout_ms)
                        .unwrap_or(false);

                    let color = if is_active {
                        POSITIVE_COLOR
                    } else {
                        NEGATIVE_COLOR
                    };
                    let age_label = last_event_ms
                        .map(|ts| format!("{}ms", (now_ms - ts).max(0)))
                        .unwrap_or_else(|| "n/a".to_owned());
                    let q_rate = health.map(|item| item.quote_rate_per_sec).unwrap_or(0.0);
                    let f_rate = health.map(|item| item.funding_rate_per_sec).unwrap_or(0.0);
                    ui.colored_label(
                        color,
                        format!(
                            "● {} Q {:.1}/s F {:.1}/s Age {}",
                            exchange.as_str(),
                            q_rate,
                            f_rate,
                            age_label
                        ),
                    );
                }
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    let column_count = 12.0_f32;
                    let spacing = ui.spacing().item_spacing.x;
                    let available = ui.available_width();
                    let dynamic_col_width = ((available - spacing * (column_count - 1.0))
                        / column_count)
                        .clamp(72.0, 170.0);

                    egui::Grid::new("arb_grid")
                        .striped(true)
                        .min_col_width(dynamic_col_width)
                        .show(ui, |ui| {
                            self.header_button(ui, "Symbol", SortColumn::Symbol);
                            self.header_button(ui, "Buy Ex", SortColumn::BuyExchange);
                            self.header_button(ui, "Sell Ex", SortColumn::SellExchange);
                            self.header_button(ui, "Funding B/S %", SortColumn::Funding);
                            self.header_button(ui, "Buy Ask", SortColumn::BuyAsk);
                            self.header_button(ui, "Sell Bid", SortColumn::SellBid);
                            self.header_button(ui, "Raw bps", SortColumn::RawSpread);
                            self.header_button(ui, "Net bps", SortColumn::NetSpread);
                            self.header_button(ui, "Max Base", SortColumn::MaxBaseQty);
                            self.header_button(ui, "Max USD", SortColumn::MaxUsdNotional);
                            self.header_button(ui, "Age ms", SortColumn::AgeMs);
                            self.header_button(ui, "Latency ms", SortColumn::LatencyMs);
                            ui.end_row();

                            for row in rows {
                                ui.label(&row.symbol);
                                Self::row_cell_exchange(ui, row.buy_ex, true);
                                Self::row_cell_exchange(ui, row.sell_ex, false);
                                Self::row_cell_funding(ui, &row);
                                ui.colored_label(NEGATIVE_COLOR, format_price_compact(row.buy_ask));
                                ui.colored_label(
                                    POSITIVE_COLOR,
                                    format_price_compact(row.sell_bid),
                                );
                                ui.colored_label(
                                    spread_color(row.raw_spread_bps),
                                    format!("{:.2}", row.raw_spread_bps),
                                );
                                ui.colored_label(
                                    spread_color(row.net_spread_bps),
                                    format!("{:.2}", row.net_spread_bps),
                                );
                                ui.label(format!("{:.6}", row.max_base_qty));
                                ui.label(format!("{:.2}", row.max_usd_notional));
                                ui.colored_label(age_color(row.age_ms), row.age_ms.to_string());
                                ui.colored_label(
                                    latency_color(row.latency_ms),
                                    row.latency_ms.to_string(),
                                );
                                ui.end_row();
                            }
                        });
                });
        });
    }
}
