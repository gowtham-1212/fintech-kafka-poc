import mongoose from 'mongoose';

const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017/payments';

const paymentSchema = new mongoose.Schema({
  transactionId: { type: String, required: true, unique: true },
  userId: { type: String, required: true },
  amount: { type: Number, required: true },
  receiverId: { type: String, required: true },
  status: { type: String, required: true },
  timestamp: { type: Date, required: true },
  processedAt: { type: Date },
  failureReason: { type: String }
}, { timestamps: true });

export const Payment = mongoose.model('Payment', paymentSchema);

export async function connectMongo() {
  try {
    await mongoose.connect(mongoUri);
    console.log('Connected to MongoDB');
  } catch (err) {
    console.error('MongoDB connection error:', err);
    process.exit(1);
  }
}
