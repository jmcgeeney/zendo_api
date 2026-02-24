import type { ReactNode } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link, useLocation, useParams } from 'react-router-dom';
import {
    Area,
    CartesianGrid,
    ComposedChart,
    Legend,
    Line,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from 'recharts';
import { getEnergySummary, getHistoricalData } from '../api/client';
import type { Customer, EnergySummary, HistoricalData, TimeSeriesPoint, WeatherData } from '../api/types';

const INTERVAL_HOURS = 0.25; // 15-minute data

function todayStr(): string {
    return new Date().toISOString().slice(0, 10);
}

function formatHHMM(value: string | number): string {
    const d = new Date(Number(value));
    return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
}

// Recharts types labelFormatter as (label: ReactNode) which is wider than
// the numeric timestamps we use, so we cast at the call site.
const labelFormatter = formatHHMM as (label: unknown) => string;

/** Returns [startOfDay, endOfDay] as Unix-ms timestamps for a YYYY-MM-DD string. */
function dayDomain(dateStr: string): [number, number] {
    const start = new Date(`${dateStr}T00:00:00`).getTime();
    return [start, start + 24 * 60 * 60 * 1000];
}

function sumKwh(series: TimeSeriesPoint[]): number {
    return series.reduce((acc, p) => acc + p.value * INTERVAL_HOURS, 0);
}

/** Stable key representing how much data a HistoricalData response contains. */
function tsFingerprint(data: HistoricalData): string {
    return `${data.production.length}:${data.production.at(-1)?.timestamp ?? ''}`;
}

type ChartPoint = Record<string, number | undefined>;

function zipSeries(
    a: TimeSeriesPoint[],
    b: TimeSeriesPoint[],
    keyA: string,
    keyB: string,
): ChartPoint[] {
    return a.map((p, i) => ({
        time: new Date(p.timestamp).getTime(),
        [keyA]: p.value,
        [keyB]: b[i]?.value,
    }));
}

function KpiTile({
    label,
    value,
    unit,
    color = '#0f172a',
}: {
    label: string;
    value: number;
    unit: string;
    color?: string;
}) {
    return (
        <div
            style={{
                flex: 1,
                minWidth: 160,
                background: '#fff',
                border: '1px solid #e2e8f0',
                borderRadius: 8,
                padding: '1rem 1.25rem',
            }}
        >
            <div
                style={{
                    fontSize: '0.75rem',
                    color: '#64748b',
                    textTransform: 'uppercase',
                    letterSpacing: '0.06em',
                    marginBottom: 4,
                }}
            >
                {label}
            </div>
            <div style={{ fontSize: '1.75rem', fontWeight: 700, color }}>
                {value.toFixed(1)}{' '}
                <span style={{ fontSize: '0.875rem', fontWeight: 400, color: '#94a3b8' }}>{unit}</span>
            </div>
        </div>
    );
}

function ChartCard({ title, children }: { title: string; children: ReactNode }) {
    return (
        <div
            style={{
                background: '#fff',
                border: '1px solid #e2e8f0',
                borderRadius: 8,
                padding: '1.25rem 1.25rem 0.75rem',
            }}
        >
            <h3 style={{ margin: '0 0 1rem', fontSize: '0.9375rem', fontWeight: 600, color: '#0f172a' }}>
                {title}
            </h3>
            {children}
        </div>
    );
}

export default function CustomerPage() {
    const { id } = useParams<{ id: string }>();
    const location = useLocation();
    const customer = location.state as Customer | null;

    const [selectedDate, setSelectedDate] = useState<string>(todayStr());
    const [timeseries, setTimeseries] = useState<HistoricalData | null>(null);
    const [energySummary, setEnergySummary] = useState<EnergySummary | null>(null);
    const [tsError, setTsError] = useState<string | null>(null);
    const [wxError, setWxError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [lastRefreshed, setLastRefreshed] = useState<Date | null>(null);
    const tsFingerprintRef = useRef<string>('');
    const summaryFingerprintRef = useRef<string>('');

    const weather = useMemo<WeatherData | null>(() => {
        if (wxError) return null;
        if (!energySummary) return null;
        return energySummary.weather_summary
    }, [wxError, energySummary]);

    const fetchData = useCallback((background = false) => {
        if (!id) return;
        const customerId = Number(id);
        if (!background) {
            // Initial load or date change — reset everything.
            setLoading(true);
            setTimeseries(null);
            setEnergySummary(null);
            tsFingerprintRef.current = '';
            summaryFingerprintRef.current = '';
        } else {
            setRefreshing(true);
        }
        setTsError(null);
        setWxError(null);
        Promise.all([
            getHistoricalData(customerId, selectedDate)
                .then((data) => {
                    const fp = tsFingerprint(data);
                    if (fp !== tsFingerprintRef.current) {
                        tsFingerprintRef.current = fp;
                        setTimeseries(data);
                    }
                })
                .catch((e: unknown) =>
                    setTsError(e instanceof Error ? e.message : 'Failed to load timeseries'),
                ),
            getEnergySummary(customerId, selectedDate)
                .then((data) => {
                    const fp = String(data.total_production_kwh);
                    if (fp !== summaryFingerprintRef.current) {
                        summaryFingerprintRef.current = fp;
                        setEnergySummary(data);
                    }
                })
                .catch((e: unknown) =>
                    setWxError(e instanceof Error ? e.message : 'Failed to load weather'),
                ),
        ]).finally(() => {
            setLoading(false);
            setRefreshing(false);
            setLastRefreshed(new Date());
        });
    }, [id, selectedDate]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
    useEffect(() => {
        if (selectedDate !== todayStr()) return;
        intervalRef.current = setInterval(() => fetchData(true), 30_000);
        return () => {
            if (intervalRef.current !== null) clearInterval(intervalRef.current);
        };
    }, [fetchData, selectedDate]);

    const domain = dayDomain(selectedDate);

    const totalProduction = timeseries ? sumKwh(timeseries.production) : 0;
    const totalConsumption = timeseries ? sumKwh(timeseries.consumption) : 0;
    const netBalance = totalProduction - totalConsumption;

    const netChartData = timeseries
        ? zipSeries(timeseries.production, timeseries.consumption, 'production', 'consumption')
        : [];
    const irradianceChartData = timeseries
        ? zipSeries(timeseries.irradiance, timeseries.production, 'irradiance', 'production')
        : [];
    const tempConsumptionData = timeseries
        ? zipSeries(timeseries.temperature, timeseries.consumption, 'temperature', 'consumption')
        : [];

    return (
        <div style={{ minHeight: '100vh', background: '#f8fafc', fontFamily: 'system-ui, sans-serif' }}>
            {/* Banner */}
            <header
                style={{
                    background: '#0f172a',
                    color: '#fff',
                    padding: '0 2rem',
                    height: 64,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                }}
            >
                <span style={{ fontSize: '1.5rem', fontWeight: 700, letterSpacing: '-0.02em' }}>
                    ⚡ Zendemo
                </span>
                <Link to="/" style={{ color: '#94a3b8', fontSize: '0.875rem', textDecoration: 'none' }}>
                    ← All Customers
                </Link>
            </header>

            <main
                style={{
                    maxWidth: 1200,
                    margin: '0 auto',
                    padding: '2.5rem 1.5rem',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '1.5rem',
                }}
            >
                {/* Customer header + weather */}
                <div
                    style={{
                        display: 'flex',
                        alignItems: 'flex-start',
                        justifyContent: 'space-between',
                        gap: '1rem',
                        flexWrap: 'wrap',
                    }}
                >
                    <div>
                        <h1 style={{ margin: 0, fontSize: '1.75rem', fontWeight: 700, color: '#0f172a' }}>
                            {customer?.name ?? `Customer ${id}`}
                        </h1>
                        {customer && (
                            <p style={{ margin: '0.25rem 0 0', color: '#64748b', fontSize: '0.875rem' }}>
                                {customer.latitude.toFixed(4)}°, {customer.longitude.toFixed(4)}°
                            </p>
                        )}
                        <div style={{ marginTop: '0.625rem', display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                            <label
                                htmlFor="date-picker"
                                style={{ fontSize: '0.8125rem', color: '#64748b', fontWeight: 500 }}
                            >
                                Date
                            </label>
                            <input
                                id="date-picker"
                                type="date"
                                value={selectedDate}
                                max={todayStr()}
                                onChange={(e) => setSelectedDate(e.target.value)}
                                style={{
                                    fontSize: '0.875rem',
                                    padding: '0.3rem 0.6rem',
                                    border: '1px solid #cbd5e1',
                                    borderRadius: 6,
                                    color: '#0f172a',
                                    background: '#fff',
                                    cursor: 'pointer',
                                }}
                            />
                            <button
                                onClick={() => fetchData(true)}
                                disabled={loading || refreshing || selectedDate !== todayStr()}
                                style={{
                                    fontSize: '0.8125rem',
                                    padding: '0.3rem 0.75rem',
                                    border: '1px solid #cbd5e1',
                                    borderRadius: 6,
                                    background: (loading || refreshing || selectedDate !== todayStr()) ? '#f1f5f9' : '#0f172a',
                                    color: (loading || refreshing || selectedDate !== todayStr()) ? '#94a3b8' : '#fff',
                                    cursor: (loading || refreshing || selectedDate !== todayStr()) ? 'not-allowed' : 'pointer',
                                    fontWeight: 500,
                                    transition: 'background 0.15s',
                                }}
                            >
                                {refreshing ? 'Refreshing…' : '↻ Refresh'}
                            </button>
                            {lastRefreshed && (
                                <span style={{ fontSize: '0.75rem', color: '#94a3b8' }}>
                                    Last updated {lastRefreshed.toLocaleTimeString()}
                                </span>
                            )}
                        </div>
                    </div>

                    {/* Weather card */}
                    <div
                        style={{
                            background: '#fff',
                            border: '1px solid #e2e8f0',
                            borderRadius: 8,
                            padding: '0.875rem 1.25rem',
                            minWidth: 200,
                        }}
                    >
                        {wxError && (
                            <span style={{ color: '#ef4444', fontSize: '0.875rem' }}>Weather unavailable</span>
                        )}
                        {!weather && !wxError && (
                            <span style={{ color: '#94a3b8', fontSize: '0.875rem' }}>Loading weather…</span>
                        )}
                        {weather && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                                <div>
                                    <div style={{ fontSize: '1.5rem', fontWeight: 700, color: '#0f172a' }}>
                                        {weather.temperature.toFixed(1)}°C
                                    </div>
                                    <div
                                        style={{
                                            fontSize: '0.8125rem',
                                            color: '#64748b',
                                            textTransform: 'capitalize',
                                        }}
                                    >
                                        {weather.description}
                                    </div>
                                    <div style={{ fontSize: '0.8125rem', color: '#94a3b8' }}>
                                        Feels like {weather.feels_like.toFixed(1)}°C
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                </div>

                {loading && <p style={{ color: '#64748b' }}>Loading data…</p>}
                {tsError && <p style={{ color: '#ef4444' }}>Error: {tsError}</p>}

                {timeseries && (
                    <>
                        {/* KPI tiles */}
                        <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
                            <KpiTile label="Total Production" value={totalProduction} unit="kWh" color="#10b981" />
                            <KpiTile label="Total Consumption" value={totalConsumption} unit="kWh" color="#f59e0b" />
                            <KpiTile
                                label="Net Balance"
                                value={netBalance}
                                unit="kWh"
                                color={netBalance >= 0 ? '#10b981' : '#ef4444'}
                            />
                        </div>

                        {/* Chart 1: Production vs Consumption */}
                        <ChartCard title="Production vs Consumption">
                            <ResponsiveContainer width="100%" height={260}>
                                <ComposedChart data={netChartData}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                                    <XAxis
                                        dataKey="time"
                                        type="number"
                                        domain={domain}
                                        tickFormatter={formatHHMM}
                                        tick={{ fontSize: 11, fill: '#94a3b8' }}
                                        minTickGap={40}
                                    />
                                    <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} unit=" kW" width={60} />
                                    <Tooltip labelFormatter={labelFormatter} />
                                    <Legend />
                                    <Area
                                        type="monotone"
                                        dataKey="production"
                                        fill="#d1fae5"
                                        stroke="#10b981"
                                        strokeWidth={2}
                                        dot={false}
                                        name="Production (kW)"
                                    />
                                    <Area
                                        type="monotone"
                                        dataKey="consumption"
                                        fill="#fef3c7"
                                        stroke="#f59e0b"
                                        strokeWidth={2}
                                        dot={false}
                                        name="Consumption (kW)"
                                    />
                                </ComposedChart>
                            </ResponsiveContainer>
                        </ChartCard>

                        {/* Chart 2: Irradiance vs Production */}
                        <ChartCard title="Solar Irradiance vs Production">
                            {timeseries.irradiance.length === 0 ? (
                                <p
                                    style={{ color: '#94a3b8', textAlign: 'center', padding: '2rem 0', margin: 0 }}
                                >
                                    No irradiance data available for this date.
                                </p>
                            ) : (
                                <ResponsiveContainer width="100%" height={260}>
                                    <ComposedChart data={irradianceChartData}>
                                        <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                                        <XAxis
                                            dataKey="time"
                                            type="number"
                                            domain={domain}
                                            tickFormatter={formatHHMM}
                                            tick={{ fontSize: 11, fill: '#94a3b8' }}
                                            minTickGap={40}
                                        />
                                        <YAxis
                                            yAxisId="left"
                                            tick={{ fontSize: 11, fill: '#94a3b8' }}
                                            unit=" W/m²"
                                            width={72}
                                        />
                                        <YAxis
                                            yAxisId="right"
                                            orientation="right"
                                            tick={{ fontSize: 11, fill: '#94a3b8' }}
                                            unit=" kW"
                                            width={56}
                                        />
                                        <Tooltip labelFormatter={labelFormatter} />
                                        <Legend />
                                        <Line
                                            yAxisId="left"
                                            type="monotone"
                                            dataKey="irradiance"
                                            stroke="#f59e0b"
                                            strokeWidth={2}
                                            dot={false}
                                            name="Irradiance (W/m²)"
                                        />
                                        <Line
                                            yAxisId="right"
                                            type="monotone"
                                            dataKey="production"
                                            stroke="#10b981"
                                            strokeWidth={2}
                                            dot={false}
                                            name="Production (kW)"
                                        />
                                    </ComposedChart>
                                </ResponsiveContainer>
                            )}
                        </ChartCard>

                        {/* Chart 3: Temperature vs Consumption */}
                        <ChartCard title="Temperature vs Consumption">
                            <ResponsiveContainer width="100%" height={260}>
                                <ComposedChart data={tempConsumptionData}>
                                    <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                                    <XAxis
                                        dataKey="time"
                                        type="number"
                                        domain={domain}
                                        tickFormatter={formatHHMM}
                                        tick={{ fontSize: 11, fill: '#94a3b8' }}
                                        minTickGap={40}
                                    />
                                    <YAxis
                                        yAxisId="left"
                                        tick={{ fontSize: 11, fill: '#94a3b8' }}
                                        unit=" °C"
                                        width={56}
                                    />
                                    <YAxis
                                        yAxisId="right"
                                        orientation="right"
                                        tick={{ fontSize: 11, fill: '#94a3b8' }}
                                        unit=" kW"
                                        width={56}
                                    />
                                    <Tooltip labelFormatter={labelFormatter} />
                                    <Legend />
                                    <Line
                                        yAxisId="left"
                                        type="monotone"
                                        dataKey="temperature"
                                        stroke="#6366f1"
                                        strokeWidth={2}
                                        dot={false}
                                        name="Temperature (°C)"
                                    />
                                    <Line
                                        yAxisId="right"
                                        type="monotone"
                                        dataKey="consumption"
                                        stroke="#f59e0b"
                                        strokeWidth={2}
                                        dot={false}
                                        name="Consumption (kW)"
                                    />
                                </ComposedChart>
                            </ResponsiveContainer>
                        </ChartCard>
                    </>
                )}
            </main>
        </div>
    );
}
