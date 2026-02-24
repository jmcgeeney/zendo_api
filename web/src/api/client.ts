import type { Customer, EnergySummary, HistoricalData } from './types';

const BASE = import.meta.env.VITE_API_BASE_URL ?? '';

async function request<T>(path: string): Promise<T> {
    const res = await fetch(`${BASE}${path}`);
    if (!res.ok) {
        throw new Error(`API error ${res.status}: ${await res.text()}`);
    }
    return res.json() as Promise<T>;
}

export const getCustomers = (): Promise<Customer[]> =>
    request('/api/customers');

export const getHistoricalData = (customerId: number, date: string): Promise<HistoricalData> =>
    request(`/api/customer/${customerId}/historical-data/${date}`);

export const getEnergySummary = (customerId: number, date: string): Promise<EnergySummary> =>
    request(`/api/customer/${customerId}/energy-summary/${date}`);