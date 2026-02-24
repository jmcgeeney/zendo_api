import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getCustomers } from '../api/client';
import type { Customer } from '../api/types';

export default function HomePage() {
    const [customers, setCustomers] = useState<Customer[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        getCustomers()
            .then(setCustomers)
            .catch((err: unknown) =>
                setError(err instanceof Error ? err.message : 'Failed to load customers'),
            )
            .finally(() => setLoading(false));
    }, []);

    return (
        <div style={{ minHeight: '100vh', background: '#f8fafc', fontFamily: 'system-ui, sans-serif' }}>
            <header
                style={{
                    background: '#0f172a',
                    color: '#fff',
                    padding: '0 2rem',
                    height: 64,
                    display: 'flex',
                    alignItems: 'center',
                }}
            >
                <span style={{ fontSize: '1.5rem', fontWeight: 700, letterSpacing: '-0.02em' }}>
                    ⚡ Zendemo
                </span>
            </header>

            <main style={{ maxWidth: 720, margin: '0 auto', padding: '2.5rem 1.5rem' }}>
                <h1 style={{ fontSize: '1.75rem', fontWeight: 700, color: '#0f172a', margin: '0 0 0.25rem' }}>
                    Customers
                </h1>
                <p style={{ color: '#64748b', margin: '0 0 2rem' }}>
                    Select a customer to view their energy dashboard.
                </p>

                {loading && <p style={{ color: '#64748b' }}>Loading…</p>}
                {error && <p style={{ color: '#ef4444' }}>Error: {error}</p>}
                {!loading && !error && customers.length === 0 && (
                    <p style={{ color: '#64748b' }}>No customers found.</p>
                )}

                <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                    {customers.map((c) => (
                        <li key={c.customer_id}>
                            <Link
                                to={`/customers/${c.customer_id}`}
                                state={c}
                                style={{
                                    display: 'flex',
                                    justifyContent: 'space-between',
                                    alignItems: 'center',
                                    padding: '1rem 1.25rem',
                                    background: '#fff',
                                    border: '1px solid #e2e8f0',
                                    borderRadius: 8,
                                    textDecoration: 'none',
                                    color: '#0f172a',
                                    fontWeight: 500,
                                }}
                            >
                                <span>{c.name}</span>
                                <span style={{ color: '#94a3b8', fontSize: '0.875rem' }}>
                                    {c.latitude.toFixed(2)}°, {c.longitude.toFixed(2)}° →
                                </span>
                            </Link>
                        </li>
                    ))}
                </ul>
            </main>
        </div>
    );
}
