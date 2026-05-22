import { useEffect, useState, useRef } from 'react';
import { 
  Activity, CheckCircle, XCircle, Clock, CreditCard, Send, Loader2, 
  LayoutDashboard, BarChart3, List, History, ShieldAlert
} from 'lucide-react';

interface KafkaEvent {
  topic: string;
  transactionId?: string;
  userId?: string;
  amount?: number;
  receiverId?: string;
  status?: 'initiated' | 'completed' | 'failed';
  timestamp?: string;
  processedAt?: string;
  failureReason?: string;
  // Metrics fields
  totalVolume?: number;
  initiatedCount?: number;
  completedCount?: number;
  failedCount?: number;
  lastUpdate?: string;
}

function App() {
  const [events, setEvents] = useState<KafkaEvent[]>([]);
  const [metrics, setMetrics] = useState<KafkaEvent | null>(null);
  const [isConnected, setIsConnected] = useState(false);
  const [amount, setAmount] = useState<string>('100.00');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [activeTab, setActiveTab] = useState<'transactions' | 'analytics'>('transactions');
  const scrollRef = useRef<HTMLDivElement>(null);

  // Remapped host ports
  const NOTIFICATION_PORT = '3003';
  const GATEWAY_PORT = '3001';

  useEffect(() => {
    const sseUrl = import.meta.env.VITE_NOTIFICATION_SERVICE_URL || `http://localhost:${NOTIFICATION_PORT}`;
    console.log('Connecting to SSE at:', sseUrl);
    const eventSource = new EventSource(`${sseUrl}/api/stream`);

    eventSource.onopen = () => setIsConnected(true);

    eventSource.onmessage = (event) => {
      try {
        const payload: KafkaEvent = JSON.parse(event.data);
        
        if (payload.topic === 'analytics.metrics') {
          setMetrics(payload);
        } else {
          setEvents((prev) => {
            const exists = prev.some(e => e.transactionId === payload.transactionId && e.status === payload.status);
            if (exists) return prev;
            return [...prev, payload];
          });
        }
      } catch (err) { }
    };

    eventSource.onerror = () => setIsConnected(false);
    return () => eventSource.close();
  }, []);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [events, activeTab]);

  const handlePay = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!amount || isNaN(Number(amount))) return;
    setIsSubmitting(true);
    try {
      const response = await fetch(`http://localhost:${GATEWAY_PORT}/api/pay`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: `user_${Math.floor(Math.random() * 10000)}`,
          amount: Number(amount),
          receiverId: `merchant_${Math.floor(Math.random() * 1000)}`
        }),
      });
      if (!response.ok) throw new Error();
    } catch (err) {
      alert('Payment failed');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex h-screen bg-[#0f172a] text-slate-200 overflow-hidden">
      {/* Sidebar */}
      <aside className="w-64 border-r border-slate-800 flex flex-col">
        <div className="p-6 border-b border-slate-800 flex items-center gap-3">
          <CreditCard className="text-indigo-500" size={28} />
          <span className="font-bold text-lg tracking-tight">Fintech Kafka</span>
        </div>
        
        <nav className="flex-1 p-4 space-y-2">
          <button 
            onClick={() => setActiveTab('transactions')}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all ${activeTab === 'transactions' ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20' : 'hover:bg-slate-800 text-slate-400'}`}
          >
            <LayoutDashboard size={20} />
            <span>Transactions</span>
          </button>
          
          <button 
            onClick={() => setActiveTab('analytics')}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all ${activeTab === 'analytics' ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-500/20' : 'hover:bg-slate-800 text-slate-400'}`}
          >
            <BarChart3 size={20} />
            <span>Real-time Analytics</span>
          </button>

          <div className="pt-8 px-4 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Kafka Clusters</div>
          <div className="mt-2 space-y-1">
            <div className="flex items-center gap-2 px-4 py-2 text-xs text-slate-400">
              <div className="w-1.5 h-1.5 rounded-full bg-green-500"></div>
              payment.initiated
            </div>
            <div className="flex items-center gap-2 px-4 py-2 text-xs text-slate-400">
              <div className="w-1.5 h-1.5 rounded-full bg-green-500"></div>
              payment.completed
            </div>
            <div className="flex items-center gap-2 px-4 py-2 text-xs text-slate-400">
              <div className="w-1.5 h-1.5 rounded-full bg-indigo-500"></div>
              analytics.metrics
            </div>
          </div>
        </nav>

        <div className="p-4 border-t border-slate-800">
          <div className={`flex items-center gap-2 px-3 py-2 rounded-full text-xs font-medium ${isConnected ? 'bg-green-500/10 text-green-400' : 'bg-red-500/10 text-red-400'}`}>
            <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}></div>
            {isConnected ? 'Connected to SSE' : 'Disconnected'}
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 flex flex-col overflow-hidden">
        <header className="h-16 border-b border-slate-800 flex items-center justify-between px-8 bg-slate-900/50 backdrop-blur-sm">
          <h2 className="text-xl font-semibold capitalize">{activeTab}</h2>
          <div className="flex items-center gap-4">
            <div className="text-xs text-slate-500 font-mono">Kafka Broker: <span className="text-indigo-400">kafka:9092</span></div>
          </div>
        </header>

        <div className="flex-1 overflow-y-auto p-8">
          {activeTab === 'transactions' ? (
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 h-full">
              <div className="lg:col-span-2 flex flex-col">
                <div className="bg-slate-900 border border-slate-800 rounded-2xl flex flex-col h-[650px] shadow-2xl">
                  <div className="p-5 border-b border-slate-800 flex justify-between items-center">
                    <div className="flex items-center gap-2">
                      <List className="text-slate-500" size={18} />
                      <h3 className="font-bold text-slate-300">Live Transaction Feed</h3>
                    </div>
                    <span className="text-[10px] bg-slate-800 px-2 py-1 rounded text-slate-400 font-mono">TOPIC: payment.*</span>
                  </div>
                  
                  <div ref={scrollRef} className="flex-1 overflow-y-auto p-5 space-y-4">
                    {events.length === 0 && (
                      <div className="h-full flex flex-col items-center justify-center text-slate-600 italic">
                        <Activity size={40} className="mb-4 animate-pulse" />
                        <p>Waiting for Kafka events...</p>
                      </div>
                    )}
                    {events.map((event, i) => (
                      <TransactionCard key={i} event={event} />
                    ))}
                  </div>
                </div>
              </div>

              <div className="space-y-6">
                <div className="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl">
                  <h3 className="font-bold mb-4 flex items-center gap-2"><Send size={18} className="text-indigo-400" /> Trigger Payment</h3>
                  <form onSubmit={handlePay} className="space-y-4">
                    <input 
                      type="number" step="0.01" value={amount} onChange={(e) => setAmount(e.target.value)}
                      className="w-full bg-black/40 border border-slate-700 rounded-xl p-3 text-xl font-bold focus:ring-2 focus:ring-indigo-500 outline-none"
                    />
                    <button disabled={isSubmitting} className="w-full bg-indigo-600 hover:bg-indigo-500 py-3 rounded-xl font-bold flex justify-center items-center gap-2 transition-all">
                      {isSubmitting ? <Loader2 className="animate-spin" /> : 'Send Transaction'}
                    </button>
                  </form>
                </div>
              </div>
            </div>
          ) : (
            <div className="space-y-8 animate-in fade-in duration-500">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                <MetricCard title="Total Volume" value={`$${metrics?.totalVolume?.toFixed(2) || '0.00'}`} color="text-indigo-400" icon={<Activity />} />
                <MetricCard title="Initiated" value={metrics?.initiatedCount || 0} color="text-blue-400" icon={<Clock />} />
                <MetricCard title="Completed" value={metrics?.completedCount || 0} color="text-green-400" icon={<CheckCircle />} />
                <MetricCard title="Failed" value={metrics?.failedCount || 0} color="text-red-400" icon={<XCircle />} />
              </div>

              <div className="bg-slate-900 border border-slate-800 rounded-2xl p-8 shadow-2xl">
                <h3 className="text-lg font-bold mb-6 flex items-center gap-2"><ShieldAlert className="text-amber-500" /> System Health</h3>
                <div className="space-y-6">
                  <ProgressBar label="Transaction Success Rate" value={metrics ? (metrics.completedCount! / metrics.initiatedCount! || 0) * 100 : 0} color="bg-green-500" />
                  <ProgressBar label="System Failure Rate" value={metrics ? (metrics.failedCount! / metrics.initiatedCount! || 0) * 100 : 0} color="bg-red-500" />
                </div>
                <div className="mt-8 text-[10px] text-slate-500 font-mono">Last update from Kafka: {metrics?.lastUpdate ? new Date(metrics.lastUpdate).toLocaleString() : 'Never'}</div>
              </div>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

function TransactionCard({ event }: { event: KafkaEvent }) {
  const isInitiated = event.status === 'initiated';
  const isCompleted = event.status === 'completed';
  const color = isInitiated ? 'border-blue-500/30 bg-blue-500/5' : isCompleted ? 'border-green-500/30 bg-green-500/5' : 'border-red-500/30 bg-red-500/5';
  
  return (
    <div className={`p-4 rounded-xl border ${color} transition-all animate-in slide-in-from-left-2`}>
      <div className="flex justify-between items-center mb-3">
        <div className="flex items-center gap-2">
          {isInitiated ? <Clock className="text-blue-400" size={16} /> : isCompleted ? <CheckCircle className="text-green-400" size={16} /> : <XCircle className="text-red-400" size={16} />}
          <span className="text-[10px] font-bold uppercase tracking-widest text-slate-500">{event.status}</span>
        </div>
        <span className="font-bold text-lg">${event.amount?.toFixed(2)}</span>
      </div>
      <div className="text-xs text-slate-400 font-mono truncate mb-2">ID: {event.transactionId}</div>
      <div className="flex justify-between text-[10px] text-slate-500 uppercase tracking-tighter">
        <span>From: {event.userId}</span>
        <span>To: {event.receiverId}</span>
      </div>
    </div>
  );
}

function MetricCard({ title, value, color, icon }: { title: string, value: string | number, color: string, icon: any }) {
  return (
    <div className="bg-slate-900 border border-slate-800 p-6 rounded-2xl shadow-xl">
      <div className="flex items-center justify-between mb-4">
        <span className="text-xs font-bold text-slate-500 uppercase tracking-widest">{title}</span>
        <div className={`${color} opacity-80`}>{icon}</div>
      </div>
      <div className={`text-2xl font-black ${color}`}>{value}</div>
    </div>
  );
}

function ProgressBar({ label, value, color }: { label: string, value: number, color: string }) {
  return (
    <div className="space-y-2">
      <div className="flex justify-between text-xs font-bold uppercase tracking-tighter text-slate-400">
        <span>{label}</span>
        <span>{value.toFixed(1)}%</span>
      </div>
      <div className="h-3 w-full bg-slate-800 rounded-full overflow-hidden">
        <div className={`h-full ${color} transition-all duration-1000`} style={{ width: `${value}%` }}></div>
      </div>
    </div>
  );
}

export default App;
