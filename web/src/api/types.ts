export interface Customer {
    customer_id: number;
    name: string;
    latitude: number;
    longitude: number;
}

export interface TimeSeriesPoint {
    timestamp: string;
    value: number;
}

export interface HistoricalData {
    customer_id: number;
    date: string;
    production: TimeSeriesPoint[];
    consumption: TimeSeriesPoint[];
    temperature: TimeSeriesPoint[];
    irradiance: TimeSeriesPoint[];
    correlation: null;
}

export interface WeatherData {
    temperature: number;
    feels_like: number;
    description: string;
    cloud_cover: number;
    wind_speed: number;
}

export interface CorrelationResponse {
    solar_irradiance_vs_production: number;
    temperature_vs_consumption: number;
}

export interface EnergySummary {
    customer_id: number;
    date: string;
    total_production_kwh: number;
    total_consumption_kwh: number;
    net_kwh: number;
    weather_summary: WeatherData;
    correlation: CorrelationResponse | null;
}