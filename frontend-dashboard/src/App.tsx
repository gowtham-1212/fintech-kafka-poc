import { useEffect, useState, useRef } from 'react';
import { Activity, CheckCircle, XCircle, Clock, CreditCard } from 'lucide-react';

interface KafkaEvent {
  topic: string;
  transactionId: string;
  userId: string;
  amount: number;
  receiverId: string;
  status: 'initiated' | 'completed' | 'failed';
  timestamp: string;
  processedAt?: string;
  failureReason?: string;
}

function App() {
  const [events, setEvents] = useState<KafkaEvent[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const sseUrl = import.meta.env.VITE_NOTIFICATION_SERVICE_URL || 'http://localhost:3003';
    console.log('Connecting to SSE at:', sseUrl);
    
    const eventSource = new EventSource(`${sseUrl}/api/stream`);

    eventSource.onopen = () => {
      console.log('SSE Connection opened');
      setIsConnected(true);
    };

    eventSource.onmessage = (event) => {
      try {
        const newEvent: KafkaEvent = JSON.parse(event.data);
        console.log('Received SSE Event:', newEvent.transactionId, newEvent.status);
        setEvents((prev) => {
          // Prevent duplicates
          const exists = prev.some(e => e.transactionId === newEvent.transactionId && e.status === newEvent.status);
          if (exists) return prev;
          return [...prev, newEvent];
        });
      } catch (err) {
        // Heartbeats are not JSON, ignore them
      }
    };

    eventSource.onerror = (err) => {
      console.error('SSE Connection Error:', err);
      setIsConnected(false);
    };

    return () => {
      eventSource.close();
    };
  }, []);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events]);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'initiated': return <Clock className="text-blue-400" size={18} />;
      case 'completed': return <CheckCircle className="text-green-400" size={18} />;
      case 'failed': return <XCircle className="text-red-400" size={18} />;
      default: return <Activity className="text-gray-400" size={18} />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'initiated': return 'border-blue-500/30 bg-blue-500/5';
      case 'completed': return 'border-green-500/30 bg-green-500/5';
      case 'failed': return 'border-red-500/30 bg-red-500/5';
      default: return 'border-gray-500/30 bg-gray-500/5';
    }
  };

  return (
    <div className="min-h-screen p-8 max-w-5xl mx-auto text-slate-200">
      <header className="mb-8 flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-3">
            <CreditCard className="text-indigo-500" />
            Fintech Kafka Monitoring
          </h1>
          <p className="text-slate-400 mt-2">Real-time event stream from Kafka topics</p>
        </div>
        <div className={`flex items-center gap-2 px-3 py-1 ${isConnected ? 'bg-green-500/10 border-green-500/20 text-green-400' : 'bg-red-500/10 border-red-500/20 text-red-400'} border rounded-full text-sm`}>
          <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}></div>
          {isConnected ? 'Live Connection' : 'Disconnected'}
        </div>
      </header>

      <div className="grid grid-cols-1 gap-6">
        <div className="bg-slate-900 border border-slate-800 rounded-xl overflow-hidden flex flex-col h-[600px]">
          <div className="p-4 border-b border-slate-800 bg-slate-900/50 flex justify-between items-center">
            <h2 className="font-semibold text-slate-300">Transaction Feed</h2>
            <span className="text-xs text-slate-500">{events.length} events received</span>
          </div>
          
          <div 
            ref={scrollRef}
            className="flex-1 overflow-y-auto p-4 space-y-3 scroll-smooth"
          >
            {events.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-slate-500 opacity-50">
                <Activity size={48} className="mb-4 animate-pulse" />
                <p>Waiting for Kafka events...</p>
                <p className="text-xs mt-2 italic">Trigger a payment via cURL to see activity</p>
              </div>
            )}
            
            {events.map((event, idx) => (
              <div 
                key={`${event.transactionId}-${event.status}-${idx}`}
                className={`p-4 rounded-lg border ${getStatusColor(event.status)} transition-all animate-in fade-in slide-in-from-bottom-2 duration-300`}
              >
                <div className="flex justify-between items-start">
                  <div className="flex items-center gap-3">
                    {getStatusIcon(event.status)}
                    <div>
                      <div className="text-sm font-mono text-slate-500 uppercase tracking-tighter">
                        {event.topic}
                      </div>
                      <div className="font-medium text-slate-200">
                        Transaction: <span className="font-mono text-xs">{event.transactionId}</span>
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-lg font-bold text-slate-100">
                      ${event.amount.toFixed(2)}
                    </div>
                    <div className="text-xs text-slate-500">
                      {new Date(event.timestamp).toLocaleTimeString()}
                    </div>
                  </div>
                </div>
                
                <div className="mt-3 pt-3 border-t border-slate-800/50 grid grid-cols-2 gap-4 text-xs">
                  <div>
                    <span className="text-slate-500">From:</span> {event.userId}
                  </div>
                  <div>
                    <span className="text-slate-500">To:</span> {event.receiverId}
                  </div>
                  {event.failureReason && (
                    <div className="col-span-2 text-red-400 italic">
                      Error: {event.failureReason}
                    </div>
                  )}
                  {event.processedAt && (
                    <div className="col-span-2 text-slate-500">
                      Processed at: {new Date(event.processedAt).toLocaleString()}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
        
        <div className="bg-slate-900 border border-slate-800 rounded-xl p-6">
          <h3 className="text-sm font-semibold text-slate-400 mb-4 uppercase tracking-wider">Quick Actions</h3>
          <div className="bg-black/40 rounded-lg p-4 font-mono text-xs text-indigo-300 overflow-x-auto">
            <pre>
{`curl -X POST http://localhost:3001/api/pay \\
  -H "Content-Type: application/json" \\
  -d '{
    "userId": "user_123",
    "amount": 250.00,
    "receiverId": "merchant_999"
  }'`}
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
