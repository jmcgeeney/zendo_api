import { BrowserRouter, Route, Routes } from 'react-router-dom';
import CustomerPage from './pages/CustomerPage';
import HomePage from './pages/HomePage';

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/customers/:id" element={<CustomerPage />} />
      </Routes>
    </BrowserRouter>
  );
}
