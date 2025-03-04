import { useState, useEffect } from "react";
import { useRouter } from "next/router";
import { ChartLine, RefreshCcw, Search, ArrowUpDown, ExternalLink } from 'lucide-react';

export default function DataSummary() {
  const router = useRouter();
  const [securities, setSecurities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [sortConfig, setSortConfig] = useState({ key: 'ticker', direction: 'ascending' });
  const [refreshing, setRefreshing] = useState(false);
  const apiBaseUrl = "http://127.0.0.1:8000";

  // Fetch securities function with async/await
  const fetchSecurities = async () => {
    setLoading(true);
    setError(null);
    
    try {
      const token = localStorage.getItem("token");
      
      if (!token) {
        router.push("/login");
        return;
      }
      
      // Debugging: log the full URL and token
      console.log('Fetching from:', `${apiBaseUrl}/securities`);
      console.log('Token:', token);
  
      const response = await fetch(`${apiBaseUrl}/securities`, {
        method: 'GET',
        headers: { 
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json'
        }
      });
  
      // Log the response status
      console.log('Response status:', response.status);
  
      if (!response.ok) {
        // Try to get error text for more information
        const errorText = await response.text();
        console.error('Error response:', errorText);
        throw new Error(errorText || "Failed to fetch securities data");
      }
      
      const data = await response.json();
      console.log('Received data:', data);
      
      setSecurities(data.securities || []);
    } catch (error) {
      console.error("Complete error details:", error);
      setError(error.message || "Failed to load securities data");
      
      // Additional network error handling
      if (error instanceof TypeError) {
        setError("Network error. Please check your connection.");
      }
    } finally {
      setLoading(false);
    }
  };

  // Refresh prices function
  const refreshPrices = async () => {
    setRefreshing(true);
    
    try {
      const token = localStorage.getItem("token");
      
      if (!token) {
        router.push("/login");
        return;
      }
      
      const response = await fetch(`${apiBaseUrl}/market/update-prices`, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}` },
      });
      
      if (!response.ok) {
        throw new Error("Failed to refresh prices");
      }
      
      const data = await response.json();
      alert(data.message);
      
      // Fetch updated securities data
      await fetchSecurities();
    } catch (error) {
      console.error("Error refreshing prices:", error);
      alert("Failed to refresh prices. Please try again later.");
    } finally {
      setRefreshing(false);
    }
  };

  // Fetch securities when component mounts
  useEffect(() => {
    fetchSecurities();
  }, []);

  // Sorting logic
  const requestSort = (key) => {
    let direction = 'ascending';
    
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    
    setSortConfig({ key, direction });
  };

  // Filtering securities
  const filteredSecurities = securities.filter(security => {
    return (
      security.ticker.toLowerCase().includes(searchTerm.toLowerCase()) ||
      (security.company_name && security.company_name.toLowerCase().includes(searchTerm.toLowerCase())) ||
      (security.sector && security.sector.toLowerCase().includes(searchTerm.toLowerCase())) ||
      (security.industry && security.industry.toLowerCase().includes(searchTerm.toLowerCase()))
    );
  });
  
  // Apply sorting
  const sortedSecurities = [...filteredSecurities].sort((a, b) => {
    if (a[sortConfig.key] === null) return 1;
    if (b[sortConfig.key] === null) return -1;
    
    if (a[sortConfig.key] < b[sortConfig.key]) {
      return sortConfig.direction === 'ascending' ? -1 : 1;
    }
    if (a[sortConfig.key] > b[sortConfig.key]) {
      return sortConfig.direction === 'ascending' ? 1 : -1;
    }
    return 0;
  });

  // Sort indicator helper
  const getSortIndicator = (key) => {
    if (sortConfig.key !== key) return null;
    return sortConfig.direction === 'ascending' ? '↑' : '↓';
  };

  // Loading state
  if (loading) {
    return (
      <div className="min-h-screen p-6 flex justify-center items-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-blue-500 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading securities data...</p>
        </div>
      </div>
    );
  }

  // Main render
  return (
    <div className="min-h-screen p-6">
      <div className="max-w-8xl mx-auto">
        <header className="mb-8">
          <h1 className="text-3xl font-bold text-blue-800 mb-2">NestEgg Data Summary</h1>
          <p className="text-gray-600">View and manage your securities data</p>
        </header>

        <div className="bg-white rounded-xl shadow-md p-6 mb-8">
          <div className="flex flex-col md:flex-row justify-between items-center mb-6">
            <div className="flex items-center mb-4 md:mb-0">
              <ChartLine className="w-6 h-6 text-blue-600 mr-2" />
              <h2 className="text-xl font-semibold">Securities Data</h2>
            </div>
            
            <div className="flex space-x-4">
              <div className="relative">
                <Search className="w-5 h-5 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search securities..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10 pr-4 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              
              <button
                onClick={refreshPrices}
                disabled={refreshing}
                className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:bg-blue-300"
              >
                {refreshing ? (
                  <>
                    <RefreshCcw className="w-5 h-5 mr-2 animate-spin" />
                    <span>Refreshing...</span>
                  </>
                ) : (
                  <>
                    <RefreshCcw className="w-5 h-5 mr-2" />
                    <span>Refresh Prices</span>
                  </>
                )}
              </button>
            </div>
          </div>

          {error && (
            <div className="bg-red-100 text-red-700 p-4 rounded-lg mb-6">
              {error}
            </div>
          )}

          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th
                    className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('ticker')}
                  >
                    <div className="flex items-center">
                      <span>Ticker</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('ticker')}
                    </div>
                  </th>
                  <th
                    className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('company_name')}
                  >
                    <div className="flex items-center">
                      <span>Company</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('company_name')}
                    </div>
                  </th>
                  <th
                    className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('sector')}
                  >
                    <div className="flex items-center">
                      <span>Sector</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('sector')}
                    </div>
                  </th>
                  <th
                    className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('price')}
                  >
                    <div className="flex items-center justify-end">
                      <span>Price</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('price')}
                    </div>
                  </th>
                  <th
                    className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('last_updated')}
                  >
                    <div className="flex items-center justify-end">
                      <span>Last Updated</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('last_updated')}
                    </div>
                  </th>
                  <th
                    className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100"
                    onClick={() => requestSort('available_on_yfinance')}
                  >
                    <div className="flex items-center justify-center">
                      <span>Available</span>
                      <ArrowUpDown className="w-4 h-4 ml-1" />
                      {getSortIndicator('available_on_yfinance')}
                    </div>
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    <span>Actions</span>
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {sortedSecurities.length > 0 ? (
                  sortedSecurities.map((security) => (
                    <tr key={security.ticker} className="hover:bg-gray-50">
                      <td className="px-4 py-4 whitespace-nowrap">
                        <div className="font-medium text-gray-900">{security.ticker}</div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap">
                        <div className="text-sm text-gray-900">{security.company_name || '-'}</div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap">
                        <div className="text-sm text-gray-900">
                          {security.sector ? (
                            <>
                              <span>{security.sector}</span>
                              {security.industry && (
                                <span className="text-xs text-gray-500 block">{security.industry}</span>
                              )}
                            </>
                          ) : (
                            '-'
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-right">
                        <div className="text-sm font-medium">
                          {security.price ? `$${security.price.toFixed(2)}` : '-'}
                        </div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-right">
                        <div className="text-sm text-gray-500">
                          {security.time_ago || 'Never'}
                          {security.last_updated && (
                            <span className="text-xs block">
                              {new Date(security.last_updated).toLocaleString()}
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-center">
                        <span
                          className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                            security.available_on_yfinance
                              ? 'bg-green-100 text-green-800'
                              : 'bg-red-100 text-red-800'
                          }`}
                        >
                          {security.available_on_yfinance ? 'Yes' : 'No'}
                        </span>
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <a 
                          href={`https://finance.yahoo.com/quote/${security.ticker}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-blue-600 hover:text-blue-900 flex items-center justify-end"
                        >
                          <span className="mr-1">View</span>
                          <ExternalLink className="w-4 h-4" />
                        </a>
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan="7" className="px-4 py-8 text-center text-gray-500">
                      {searchTerm
                        ? 'No securities match your search criteria'
                        : 'No securities data available'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>

          <div className="mt-4 text-sm text-gray-500">
            Total: {filteredSecurities.length} securities
            {filteredSecurities.length !== securities.length && (
              <span> (filtered from {securities.length})</span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}