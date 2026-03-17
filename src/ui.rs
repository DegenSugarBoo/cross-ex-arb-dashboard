use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU32, Ordering as AtomicOrdering},
};
use std::time::Duration;

use eframe::egui::{self, Color32};
use egui_plot::{CoordinatesFormatter, Corner, Line, Plot, PlotPoints};
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::config::AppConfig;
use crate::model::{
    ArbRow, Exchange, ExchangeFeedHealth, NO_ROUTE_SELECTED, RouteHistorySnapshot, now_ms,
};

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
    selected_route_id: Arc<AtomicU32>,
    detail_snapshot: Arc<RwLock<Option<RouteHistorySnapshot>>>,
    frame_delay: Duration,
    sort_column: SortColumn,
    sort_descending: bool,
    search_query: String,
    all_rows_buf: Vec<ArbRow>,
    table_rows_buf: Vec<ArbRow>,
    search_match_indices: Vec<usize>,
    detail_window_open: bool,
    detail_revision_seen: u64,
    detail_cache: Option<RouteHistorySnapshot>,
    metrics_pid: Option<Pid>,
    metrics_system: System,
    metrics_last_refresh_ms: i64,
    metrics_rss_bytes: u64,
    metrics_virtual_bytes: u64,
}

const POSITIVE_COLOR: Color32 = Color32::from_rgb(70, 170, 95);
const NEGATIVE_COLOR: Color32 = Color32::from_rgb(205, 75, 75);
const STALE_COLOR: Color32 = Color32::from_rgb(215, 160, 65);
const AGE_OK_MS: i64 = 1_200;
const LATENCY_OK_MS: i64 = 300;
const EXCHANGE_DOT_FRESH_MS: i64 = 5_000;
const TABLE_ROW_LIMIT: usize = 20;
const RUNTIME_METRICS_REFRESH_MS: i64 = 1_000;

impl ArbApp {
    pub fn new(
        snapshot: Arc<RwLock<Vec<ArbRow>>>,
        exchange_health: Arc<RwLock<HashMap<Exchange, ExchangeFeedHealth>>>,
        selected_route_id: Arc<AtomicU32>,
        detail_snapshot: Arc<RwLock<Option<RouteHistorySnapshot>>>,
        config: AppConfig,
    ) -> Self {
        let fps = config.ui_fps.max(1);
        let metrics_pid = sysinfo::get_current_pid().ok();
        let mut metrics_system = System::new();
        let mut metrics_rss_bytes = 0_u64;
        let mut metrics_virtual_bytes = 0_u64;
        if let Some(pid) = metrics_pid {
            metrics_system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
            if let Some(process) = metrics_system.process(pid) {
                metrics_rss_bytes = process.memory();
                metrics_virtual_bytes = process.virtual_memory();
            }
        }
        Self {
            snapshot,
            exchange_health,
            selected_route_id,
            detail_snapshot,
            frame_delay: Duration::from_millis((1000 / fps) as u64),
            sort_column: SortColumn::NetSpread,
            sort_descending: true,
            search_query: String::new(),
            all_rows_buf: Vec::new(),
            table_rows_buf: Vec::new(),
            search_match_indices: Vec::new(),
            detail_window_open: false,
            detail_revision_seen: 0,
            detail_cache: None,
            metrics_pid,
            metrics_system,
            metrics_last_refresh_ms: 0,
            metrics_rss_bytes,
            metrics_virtual_bytes,
        }
    }

    fn refresh_all_rows_snapshot(&mut self) {
        let guard = match self.snapshot.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        self.all_rows_buf.clear();
        self.all_rows_buf.extend_from_slice(&guard);
    }

    fn rebuild_table_rows(&mut self) {
        rebuild_table_rows_from_all(&self.all_rows_buf, &mut self.table_rows_buf);
    }

    fn refresh_detail_snapshot(&mut self) {
        let guard = match self.detail_snapshot.read() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };

        match guard.as_ref() {
            Some(snapshot) => {
                if snapshot.revision != self.detail_revision_seen {
                    self.detail_revision_seen = snapshot.revision;
                    self.detail_cache = Some(snapshot.clone());
                }
            }
            None => {
                self.detail_revision_seen = 0;
                self.detail_cache = None;
            }
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

    fn sort_table_rows_buf(&mut self) {
        if self.sort_column == SortColumn::NetSpread && self.sort_descending {
            return;
        }

        let sort_column = self.sort_column;
        let sort_descending = self.sort_descending;
        self.table_rows_buf.sort_unstable_by(|left, right| {
            let order = match sort_column {
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

            if sort_descending {
                order.reverse()
            } else {
                order
            }
        });
    }

    fn open_route_detail(&mut self, route_id: u32) {
        self.selected_route_id
            .store(route_id, AtomicOrdering::Relaxed);
        self.detail_window_open = true;
        self.detail_revision_seen = 0;
        self.detail_cache = None;
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

    fn detail_title(&self) -> String {
        if let Some(snapshot) = &self.detail_cache {
            return format!(
                "Route Detail: {} | {} -> {}",
                snapshot.key.symbol, snapshot.key.buy_ex, snapshot.key.sell_ex
            );
        }

        let selected = self.selected_route_id.load(AtomicOrdering::Relaxed);
        if selected == NO_ROUTE_SELECTED {
            "Route Detail".to_owned()
        } else {
            format!("Route Detail: route #{selected} (loading)")
        }
    }

    fn render_detail_window(&mut self, ctx: &egui::Context) {
        if !self.detail_window_open {
            return;
        }

        let mut open = self.detail_window_open;
        let title = self.detail_title();
        egui::Window::new(title)
            .open(&mut open)
            .resizable(true)
            .default_size(egui::vec2(780.0, 460.0))
            .show(ctx, |ui| {
                if let Some(snapshot) = &self.detail_cache {
                    if snapshot.points.is_empty() {
                        ui.label("No history points yet for this route.");
                        return;
                    }

                    let label_height = ui.text_style_height(&egui::TextStyle::Body);
                    let spacing_y = ui.spacing().item_spacing.y;
                    let reserved_height = 3.0 * label_height + 4.0 * spacing_y;
                    let plot_total_height = (ui.available_height() - reserved_height).max(1.0);
                    let unit = plot_total_height / 4.0;
                    let top_height = 2.0 * unit;
                    let mid_height = unit;
                    let bot_height = unit;
                    let plot_group_id = egui::Id::new(("route_detail_plots", snapshot.route_id));

                    let net_points: PlotPoints<'static> = snapshot
                        .points
                        .iter()
                        .map(|point| {
                            [
                                ((point.ts_ms - snapshot.generated_at_ms) as f64 / 1_000.0)
                                    .clamp(-30.0, 0.0),
                                point.net_spread_bps as f64,
                            ]
                        })
                        .collect();
                    let max_usd_points: PlotPoints<'static> = snapshot
                        .points
                        .iter()
                        .map(|point| {
                            [
                                ((point.ts_ms - snapshot.generated_at_ms) as f64 / 1_000.0)
                                    .clamp(-30.0, 0.0),
                                point.max_usd_notional as f64,
                            ]
                        })
                        .collect();
                    let age_points: PlotPoints<'static> = snapshot
                        .points
                        .iter()
                        .map(|point| {
                            [
                                ((point.ts_ms - snapshot.generated_at_ms) as f64 / 1_000.0)
                                    .clamp(-30.0, 0.0),
                                point.age_ms as f64,
                            ]
                        })
                        .collect();

                    let formatter = CoordinatesFormatter::new(|point, _bounds| {
                        format!("x: {:.2}s\ny: {:.3}", point.x, point.y)
                    });

                    ui.label("Spread history (net_spread_bps)");
                    Plot::new(("route_detail_net", snapshot.route_id))
                        .height(top_height)
                        .include_x(-30.0)
                        .include_x(0.0)
                        .link_axis(plot_group_id, [true, false])
                        .link_cursor(plot_group_id, [true, false])
                        .allow_scroll([true, false])
                        .allow_zoom([true, false])
                        .coordinates_formatter(Corner::LeftTop, formatter)
                        .label_formatter(|name, value| {
                            format!("{name}\nx: {:.2}s\ny: {:.3}", value.x, value.y)
                        })
                        .show(ui, |plot_ui| {
                            plot_ui.line(
                                Line::new(net_points)
                                    .name("net_spread_bps")
                                    .color(POSITIVE_COLOR)
                                    .width(1.8),
                            );
                        });

                    ui.add_space(4.0);
                    ui.label("Max USD (max_usd_notional)");
                    Plot::new(("route_detail_usd", snapshot.route_id))
                        .height(mid_height)
                        .include_x(-30.0)
                        .include_x(0.0)
                        .link_axis(plot_group_id, [true, false])
                        .link_cursor(plot_group_id, [true, false])
                        .allow_scroll([true, false])
                        .allow_zoom([true, false])
                        .coordinates_formatter(
                            Corner::LeftTop,
                            CoordinatesFormatter::new(|point, _bounds| {
                                format!("x: {:.2}s\ny: {:.2}", point.x, point.y)
                            }),
                        )
                        .label_formatter(|name, value| {
                            format!("{name}\nx: {:.2}s\ny: {:.2}", value.x, value.y)
                        })
                        .show(ui, |plot_ui| {
                            plot_ui.line(
                                Line::new(max_usd_points)
                                    .name("max_usd_notional")
                                    .color(Color32::from_rgb(86, 136, 230))
                                    .width(1.6),
                            );
                        });

                    ui.add_space(4.0);
                    ui.label("Age (age_ms)");
                    Plot::new(("route_detail_age", snapshot.route_id))
                        .height(bot_height)
                        .include_x(-30.0)
                        .include_x(0.0)
                        .link_axis(plot_group_id, [true, false])
                        .link_cursor(plot_group_id, [true, false])
                        .allow_scroll([true, false])
                        .allow_zoom([true, false])
                        .coordinates_formatter(
                            Corner::LeftTop,
                            CoordinatesFormatter::new(|point, _bounds| {
                                format!("x: {:.2}s\ny: {:.1}ms", point.x, point.y)
                            }),
                        )
                        .label_formatter(|name, value| {
                            format!("{name}\nx: {:.2}s\ny: {:.1}ms", value.x, value.y)
                        })
                        .show(ui, |plot_ui| {
                            plot_ui.line(
                                Line::new(age_points)
                                    .name("age_ms")
                                    .color(STALE_COLOR)
                                    .width(1.6),
                            );
                        });
                } else {
                    ui.label("Waiting for selected route history...");
                }
            });

        self.detail_window_open = open;
        if !self.detail_window_open {
            self.selected_route_id
                .store(NO_ROUTE_SELECTED, AtomicOrdering::Relaxed);
            self.detail_cache = None;
            self.detail_revision_seen = 0;
        }
    }

    fn refresh_runtime_metrics(&mut self, now_ms: i64) {
        if now_ms - self.metrics_last_refresh_ms < RUNTIME_METRICS_REFRESH_MS {
            return;
        }
        self.metrics_last_refresh_ms = now_ms;

        let Some(pid) = self.metrics_pid else {
            return;
        };

        self.metrics_system
            .refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
        if let Some(process) = self.metrics_system.process(pid) {
            self.metrics_rss_bytes = process.memory();
            self.metrics_virtual_bytes = process.virtual_memory();
        }
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

fn format_age_label(last_event_ms: Option<i64>, now_ms: i64) -> String {
    last_event_ms
        .map(|ts| format!("{}ms", (now_ms - ts).max(0)))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn exchange_is_live(health: Option<&ExchangeFeedHealth>, now_ms: i64) -> bool {
    let freshness_anchor_ms = health.and_then(|item| item.last_quote_ms.or(item.last_event_ms));
    freshness_anchor_ms
        .map(|ts| (now_ms - ts).max(0) <= EXCHANGE_DOT_FRESH_MS)
        .unwrap_or(false)
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

fn normalize_search_query(query: &str) -> &str {
    query.trim()
}

fn format_bytes_compact(bytes: u64) -> String {
    let mib = (bytes as f64) / (1024.0 * 1024.0);
    if mib >= 1024.0 {
        format!("{:.2} GiB", mib / 1024.0)
    } else {
        format!("{mib:.1} MiB")
    }
}

fn match_tier(symbol_ascii_lower: &str, needle_ascii_lower: &str) -> Option<u8> {
    if symbol_ascii_lower == needle_ascii_lower {
        Some(0)
    } else if symbol_ascii_lower.starts_with(needle_ascii_lower) {
        Some(1)
    } else if symbol_ascii_lower.contains(needle_ascii_lower) {
        Some(2)
    } else {
        None
    }
}

fn rank_cmp_for_search(left: &ArbRow, right: &ArbRow) -> Ordering {
    cmp_f64(right.net_spread_bps, left.net_spread_bps)
        .then_with(|| cmp_f64(right.raw_spread_bps, left.raw_spread_bps))
        .then_with(|| left.symbol.cmp(&right.symbol))
}

fn rebuild_search_match_indices(rows: &[ArbRow], query: &str, out: &mut Vec<usize>) {
    out.clear();

    let normalized = normalize_search_query(query);
    if normalized.is_empty() {
        return;
    }

    let needle = normalized.to_ascii_lowercase();
    let mut ranked_matches: Vec<(u8, usize)> = Vec::new();
    for (idx, row) in rows.iter().enumerate() {
        let symbol_ascii_lower = row.symbol.to_ascii_lowercase();
        if let Some(tier) = match_tier(&symbol_ascii_lower, &needle) {
            ranked_matches.push((tier, idx));
        }
    }

    ranked_matches.sort_unstable_by(|(left_tier, left_idx), (right_tier, right_idx)| {
        left_tier
            .cmp(right_tier)
            .then_with(|| rank_cmp_for_search(&rows[*left_idx], &rows[*right_idx]))
    });
    out.extend(ranked_matches.into_iter().map(|(_, idx)| idx));
}

fn rebuild_table_rows_from_all(all_rows: &[ArbRow], table_rows: &mut Vec<ArbRow>) {
    table_rows.clear();
    table_rows.extend(all_rows.iter().take(TABLE_ROW_LIMIT).cloned());
}

impl eframe::App for ArbApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        ctx.request_repaint_after(self.frame_delay);

        self.refresh_all_rows_snapshot();
        self.rebuild_table_rows();
        self.refresh_detail_snapshot();
        self.sort_table_rows_buf();
        let row_count = self.table_rows_buf.len();
        let frame_now_ms = now_ms();
        self.refresh_runtime_metrics(frame_now_ms);

        let mut clicked_route_id: Option<u32> = None;
        let mut clear_search_after_click = false;
        egui::TopBottomPanel::top("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Cross-Exchange Arb Scanner");
                ui.separator();
                ui.label(format!("Rows: {row_count}"));
            });

            ui.add_space(4.0);
            let health_guard = match self.exchange_health.read() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            ui.scope(|ui| {
                ui.spacing_mut().item_spacing = egui::vec2(4.0, 3.0);
                ui.horizontal_wrapped(|ui| {
                    for exchange in Exchange::all() {
                        let health = health_guard.get(exchange);
                        let dot_color = if exchange_is_live(health, frame_now_ms) {
                            POSITIVE_COLOR
                        } else {
                            NEGATIVE_COLOR
                        };
                        let age_label =
                            format_age_label(health.and_then(|item| item.last_event_ms), frame_now_ms);
                        let quote_rate = health.map(|item| item.quote_rate_per_sec).unwrap_or(0.0);
                        let funding_rate =
                            health.map(|item| item.funding_rate_per_sec).unwrap_or(0.0);

                        egui::Frame::group(ui.style())
                            .inner_margin(3)
                            .outer_margin(0)
                            .show(ui, |ui| {
                                ui.vertical(|ui| {
                                    let id =
                                        ui.make_persistent_id(("exchange_status", exchange.index()));
                                    let mut state = egui::collapsing_header::CollapsingState::load_with_default_open(
                                        ui.ctx(),
                                        id,
                                        false,
                                    );
                                    ui.horizontal(|ui| {
                                        ui.spacing_mut().item_spacing.x = 2.0;
                                        state.show_toggle_button(
                                            ui,
                                            egui::collapsing_header::paint_default_icon,
                                        );
                                        let name_response = ui.add(
                                            egui::Label::new(exchange.as_str())
                                                .sense(egui::Sense::click()),
                                        );
                                        let dot_response = ui.add(
                                            egui::Label::new(
                                                egui::RichText::new("●").color(dot_color),
                                            )
                                            .sense(egui::Sense::click()),
                                        );
                                        if name_response.clicked() || dot_response.clicked() {
                                            state.toggle(ui);
                                        }
                                    });

                                    state.show_body_unindented(ui, |ui| {
                                        ui.horizontal_wrapped(|ui| {
                                            ui.label(format!("Q {:.1}/s", quote_rate));
                                            ui.separator();
                                            ui.label(format!("F {:.1}/s", funding_rate));
                                            ui.separator();
                                            ui.label(format!("Age {age_label}"));
                                        });
                                    });
                                });
                            });
                    }

                    let mut total_quote_events: u64 = 0;
                    let mut total_funding_events: u64 = 0;
                    let mut last_collected_ms: Option<i64> = None;
                    for health in health_guard.values() {
                        total_quote_events =
                            total_quote_events.saturating_add(health.total_quote_events);
                        total_funding_events =
                            total_funding_events.saturating_add(health.total_funding_events);
                        let candidate = health.last_event_ms;
                        last_collected_ms = match (last_collected_ms, candidate) {
                            (Some(existing), Some(next)) => Some(existing.max(next)),
                            (None, Some(next)) => Some(next),
                            (existing, None) => existing,
                        };
                    }
                    let total_events = total_quote_events.saturating_add(total_funding_events);
                    let data_dot_color = if total_events > 0 {
                        POSITIVE_COLOR
                    } else {
                        STALE_COLOR
                    };
                    let data_age_label = format_age_label(last_collected_ms, frame_now_ms);
                    let rss_label = if self.metrics_pid.is_some() {
                        format!("RSS {}", format_bytes_compact(self.metrics_rss_bytes))
                    } else {
                        "RSS n/a".to_owned()
                    };
                    let vm_label = if self.metrics_pid.is_some() {
                        format!("VM {}", format_bytes_compact(self.metrics_virtual_bytes))
                    } else {
                        "VM n/a".to_owned()
                    };

                    egui::Frame::group(ui.style())
                        .inner_margin(3)
                        .outer_margin(0)
                        .show(ui, |ui| {
                            ui.vertical(|ui| {
                                let id = ui.make_persistent_id("runtime_status_card");
                                let mut state =
                                    egui::collapsing_header::CollapsingState::load_with_default_open(
                                        ui.ctx(),
                                        id,
                                        false,
                                    );
                                ui.horizontal(|ui| {
                                    ui.spacing_mut().item_spacing.x = 2.0;
                                    state.show_toggle_button(
                                        ui,
                                        egui::collapsing_header::paint_default_icon,
                                    );
                                    let name_response = ui.add(
                                        egui::Label::new("Data").sense(egui::Sense::click()),
                                    );
                                    let dot_response = ui.add(
                                        egui::Label::new(
                                            egui::RichText::new("●").color(data_dot_color),
                                        )
                                        .sense(egui::Sense::click()),
                                    );
                                    if name_response.clicked() || dot_response.clicked() {
                                        state.toggle(ui);
                                    }
                                });

                                state.show_body_unindented(ui, |ui| {
                                    ui.horizontal_wrapped(|ui| {
                                        ui.label(format!("Events {total_events}"));
                                        ui.separator();
                                        ui.label(format!("Q {total_quote_events}"));
                                        ui.separator();
                                        ui.label(format!("F {total_funding_events}"));
                                        ui.separator();
                                        ui.label(format!("Age {data_age_label}"));
                                        ui.separator();
                                        ui.label(rss_label);
                                        ui.separator();
                                        ui.label(vm_label);
                                        ui.separator();
                                        ui.label(format!("Rows {}", self.all_rows_buf.len()));
                                        ui.separator();
                                        ui.label(format!("Shown {row_count}"));
                                        ui.separator();
                                        ui.label("History window 30s");
                                    });
                                });
                            });
                        });
                });
            });

            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("Search Symbol");
                ui.add_sized(
                    [280.0, 22.0],
                    egui::TextEdit::singleline(&mut self.search_query)
                        .hint_text("Type symbol (e.g. btc, ETH)"),
                );
            });

            rebuild_search_match_indices(
                &self.all_rows_buf,
                &self.search_query,
                &mut self.search_match_indices,
            );

            if !normalize_search_query(&self.search_query).is_empty() {
                egui::Frame::group(ui.style())
                    .inner_margin(6)
                    .show(ui, |ui| {
                        ui.set_min_width(460.0);
                        if self.search_match_indices.is_empty() {
                            ui.label("No matches");
                            return;
                        }

                        for &match_idx in &self.search_match_indices {
                            let row = &self.all_rows_buf[match_idx];
                            let label = format!(
                                "{}  |  {} -> {}  |  {:+.2} bps",
                                row.symbol, row.buy_ex, row.sell_ex, row.net_spread_bps
                            );
                            if ui.selectable_label(false, label).clicked() {
                                clicked_route_id = Some(row.route_id);
                                clear_search_after_click = true;
                            }
                        }
                    });
            }
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

                            for row in &self.table_rows_buf {
                                if ui.link(row.symbol.as_str()).clicked() {
                                    clicked_route_id = Some(row.route_id);
                                }
                                Self::row_cell_exchange(ui, row.buy_ex, true);
                                Self::row_cell_exchange(ui, row.sell_ex, false);
                                Self::row_cell_funding(ui, row);
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

        if let Some(route_id) = clicked_route_id {
            self.open_route_detail(route_id);
            if clear_search_after_click {
                self.search_query.clear();
                self.search_match_indices.clear();
            }
        }

        self.render_detail_window(ctx);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{
        Arc, RwLock,
        atomic::{AtomicU32, Ordering as AtomicOrdering},
    };
    use std::time::Duration;

    use super::{
        ArbApp, SortColumn, normalize_search_query, rebuild_search_match_indices,
        rebuild_table_rows_from_all,
    };
    use crate::model::{
        ArbRow, Exchange, ExchangeFeedHealth, NO_ROUTE_SELECTED, RouteHistorySnapshot, RouteKey,
    };

    fn row(route_id: u32, symbol: &str, net_spread_bps: f64, raw_spread_bps: f64) -> ArbRow {
        ArbRow {
            route_id,
            symbol: symbol.to_owned().into(),
            buy_ex: Exchange::Lighter,
            sell_ex: Exchange::Aster,
            buy_ask: 100.0,
            sell_bid: 101.0,
            raw_spread_bps,
            net_spread_bps,
            buy_funding_rate: None,
            sell_funding_rate: None,
            buy_funding_stale: false,
            sell_funding_stale: false,
            max_base_qty: 1.0,
            max_usd_notional: 100.0,
            age_ms: 10,
            latency_ms: 5,
        }
    }

    fn make_test_app() -> ArbApp {
        ArbApp {
            snapshot: Arc::new(RwLock::new(Vec::new())),
            exchange_health: Arc::new(RwLock::new(HashMap::<Exchange, ExchangeFeedHealth>::new())),
            selected_route_id: Arc::new(AtomicU32::new(NO_ROUTE_SELECTED)),
            detail_snapshot: Arc::new(RwLock::new(None)),
            frame_delay: Duration::from_millis(16),
            sort_column: SortColumn::NetSpread,
            sort_descending: true,
            search_query: String::new(),
            all_rows_buf: Vec::new(),
            table_rows_buf: Vec::new(),
            search_match_indices: Vec::new(),
            detail_window_open: false,
            detail_revision_seen: 0,
            detail_cache: None,
            metrics_pid: None,
            metrics_system: sysinfo::System::new(),
            metrics_last_refresh_ms: 0,
            metrics_rss_bytes: 0,
            metrics_virtual_bytes: 0,
        }
    }

    #[test]
    fn search_matching_is_case_insensitive_substring() {
        let rows = vec![
            row(1, "BTC", 20.0, 20.0),
            row(2, "ETH", 19.0, 19.0),
            row(3, "SOL", 18.0, 18.0),
        ];
        let mut matches = Vec::new();

        rebuild_search_match_indices(&rows, "  eT  ", &mut matches);

        assert_eq!(normalize_search_query("  eT  "), "eT");
        assert_eq!(matches.len(), 1);
        assert_eq!(rows[matches[0]].symbol, "ETH");
    }

    #[test]
    fn search_results_are_ranked_by_net_then_raw_then_symbol() {
        let rows = vec![
            row(1, "AX1", 10.0, 100.0),
            row(2, "AX2", 12.0, 90.0),
            row(3, "AX0", 10.0, 100.0),
            row(4, "BY0", 20.0, 20.0),
        ];
        let mut matches = Vec::new();

        rebuild_search_match_indices(&rows, "ax", &mut matches);

        let symbols: Vec<&str> = matches
            .iter()
            .map(|idx| rows[*idx].symbol.as_str())
            .collect();
        assert_eq!(symbols, vec!["AX2", "AX0", "AX1"]);
    }

    #[test]
    fn search_returns_all_matches() {
        let rows: Vec<ArbRow> = (0..15)
            .map(|idx| {
                row(
                    idx as u32,
                    &format!("COIN{idx:02}"),
                    (100 - idx) as f64,
                    1.0,
                )
            })
            .collect();
        let mut matches = Vec::new();

        rebuild_search_match_indices(&rows, "coin", &mut matches);

        assert_eq!(matches.len(), 15);
    }

    #[test]
    fn exact_symbol_match_is_prioritized_even_if_net_is_negative() {
        let mut rows = vec![row(1, "BTC", -12.0, -10.0)];
        for idx in 0..12 {
            rows.push(row(
                (idx + 2) as u32,
                &format!("BTC{idx:02}"),
                100.0 - idx as f64,
                1.0,
            ));
        }

        let mut matches = Vec::new();
        rebuild_search_match_indices(&rows, "btc", &mut matches);

        assert_eq!(rows[matches[0]].symbol, "BTC");
        assert_eq!(rows[matches[0]].net_spread_bps, -12.0);
        assert_eq!(matches.len(), 13);
    }

    #[test]
    fn search_lists_all_routes_for_same_symbol_sorted_by_net() {
        let rows = vec![
            row(1, "BTC", 8.0, 8.0),
            row(2, "BTC", 12.0, 12.0),
            row(3, "BTC", 5.0, 5.0),
            row(4, "ETH", 20.0, 20.0),
        ];
        let mut matches = Vec::new();

        rebuild_search_match_indices(&rows, "btc", &mut matches);

        assert_eq!(matches.len(), 3);
        let sorted_symbols: Vec<&str> = matches
            .iter()
            .map(|idx| rows[*idx].symbol.as_str())
            .collect();
        let sorted_nets: Vec<f64> = matches
            .iter()
            .map(|idx| rows[*idx].net_spread_bps)
            .collect();
        assert_eq!(sorted_symbols, vec!["BTC", "BTC", "BTC"]);
        assert_eq!(sorted_nets, vec![12.0, 8.0, 5.0]);
    }

    #[test]
    fn open_route_detail_updates_shared_selection_and_detail_state() {
        let selected_route_id = Arc::new(AtomicU32::new(NO_ROUTE_SELECTED));
        let mut app = make_test_app();
        app.selected_route_id = Arc::clone(&selected_route_id);
        app.detail_window_open = false;
        app.detail_revision_seen = 88;
        app.detail_cache = Some(RouteHistorySnapshot {
            route_id: 9,
            key: RouteKey {
                symbol: "BTC".to_owned().into(),
                buy_ex: Exchange::Lighter,
                sell_ex: Exchange::Aster,
            },
            points: Vec::new(),
            generated_at_ms: 123,
            revision: 7,
        });

        app.open_route_detail(42);

        assert_eq!(selected_route_id.load(AtomicOrdering::Relaxed), 42);
        assert!(app.detail_window_open);
        assert_eq!(app.detail_revision_seen, 0);
        assert!(app.detail_cache.is_none());
    }

    #[test]
    fn table_rebuild_uses_top_twenty_prefix_then_sorts_within_subset() {
        let mut all_rows: Vec<ArbRow> = (0..25)
            .map(|idx| row(idx as u32, &format!("ZZ{idx:02}"), (100 - idx) as f64, 1.0))
            .collect();
        all_rows[22].symbol = "AAA".to_owned().into();

        let mut app = make_test_app();
        app.all_rows_buf = all_rows;
        app.sort_column = SortColumn::Symbol;
        app.sort_descending = false;

        app.rebuild_table_rows();
        app.sort_table_rows_buf();

        assert_eq!(app.table_rows_buf.len(), 20);
        assert!(app.table_rows_buf.iter().all(|entry| entry.route_id < 20));
        assert!(!app.table_rows_buf.iter().any(|entry| entry.symbol == "AAA"));

        let mut direct_rebuild = Vec::new();
        rebuild_table_rows_from_all(&app.all_rows_buf, &mut direct_rebuild);
        assert_eq!(direct_rebuild.len(), 20);
    }
}
