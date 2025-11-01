// Load environment variables
require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const jwt = require("jsonwebtoken");
const { MongoClient, ObjectId, ServerApiVersion } = require("mongodb");
// const { body, validationResult } = require('express-validator');

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors({
  origin: [
    'http://localhost:5173', // Vite dev server
    'https://erp-dashboard1.netlify.app' // Netlify production
  ],
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Middleware to ensure database is initialized before handling requests
app.use(async (req, res, next) => {
  try {
    await ensureDatabaseInitialized();

    // If database connection failed, return appropriate error for database-dependent routes
    if (dbConnectionError && req.path !== '/') {
      return res.status(503).json({
        success: false,
        error: 'Database connection failed',
        message: 'Please check your MongoDB credentials in .env file',
        details: dbConnectionError.message
      });
    }

    next();
  } catch (error) {
    console.error('Database initialization error:', error);
    res.status(500).json({
      success: false,
      error: 'Database connection failed',
      message: 'Please try again later'
    });
  }
});

// ✅ POST: Complete existing transaction (idempotent + atomic)
// Assumes you have: db, collections: transactions, agents, customers, vendors, invoices, accounts
// and ObjectId from mongodb driver in scope.

app.post("/api/transactions/:id/complete", async (req, res) => {
  let session = null;
  try {
    const { id } = req.params;
    const { amount: bodyAmount, paymentDetails } = req.body || {};

    // 1) Find transaction by _id or transactionId
    let tx = null;
    if (ObjectId.isValid(id)) {
      tx = await transactions.findOne({ _id: new ObjectId(id), isActive: { $ne: false } });
    }
    if (!tx) {
      tx = await transactions.findOne({ transactionId: String(id), isActive: { $ne: false } });
    }
    if (!tx) {
      return res.status(404).json({ success: false, message: "Transaction not found" });
    }

    // Early return if already completed (idempotent)
    if (tx.status === 'completed') {
      // Return the current party snapshots (optional)
      let agent = null, customer = null, vendor = null, invoice = null, sourceAccount = null, targetAccount = null;

      if (tx.partyType === 'agent' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ agentId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ agentId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        agent = await agents.findOne(cond);
      } else if (tx.partyType === 'customer' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        customer = await customers.findOne(cond);
      } else if (tx.partyType === 'vendor' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ vendorId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ vendorId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        vendor = await vendors.findOne(cond);
      }

      if (tx.invoiceId) {
        const invCond = ObjectId.isValid(tx.invoiceId)
          ? { $or: [{ invoiceId: tx.invoiceId }, { _id: new ObjectId(tx.invoiceId) }], isActive: { $ne: false } }
          : { $or: [{ invoiceId: tx.invoiceId }, { _id: tx.invoiceId }], isActive: { $ne: false } };
        invoice = await invoices.findOne(invCond);
      }

      if (tx.sourceAccountId) {
        sourceAccount = await accounts.findOne({ _id: new ObjectId(tx.sourceAccountId) }).catch(() => null);
      }
      if (tx.targetAccountId) {
        targetAccount = await accounts.findOne({ _id: new ObjectId(tx.targetAccountId) }).catch(() => null);
      }

      return res.json({
        success: true,
        message: 'Already completed',
        transaction: tx,
        agent, customer, vendor, invoice,
        sourceAccount, targetAccount
      });
    }

    // 2) Start session for atomic updates
    session = db.client.startSession();
    session.startTransaction();

    let updatedAgent = null;
    let updatedCustomer = null;
    let updatedVendor = null;
    let updatedInvoice = null;
    let updatedSourceAccount = null;
    let updatedTargetAccount = null;

    try {
      // Normalize numeric amount (allow body override; if invalid, skip aggregate math but still mark completed)
      const numericAmount = parseFloat(
        (bodyAmount !== undefined ? bodyAmount : (paymentDetails?.amount))
        ?? tx.amount
        ?? tx?.paymentDetails?.amount
        ?? 0
      );
      const hasValidAmount = !isNaN(numericAmount) && numericAmount > 0;

      // Determine account IDs (fallback to nested objects if needed)
      const sourceAccountId =
        tx.sourceAccountId ||
        tx.debitAccount?.id ||
        null;
      const targetAccountId =
        tx.targetAccountId ||
        tx.creditAccount?.id ||
        null;

      const transactionType = tx.transactionType; // credit | debit | transfer
      const partyType = tx.partyType;             // agent | customer | vendor
      const serviceCategory = tx.serviceCategory || tx.category || tx?.meta?.selectedOption || '';
      const categoryText = String(serviceCategory).toLowerCase();
      const isHajjCategory = categoryText.includes('haj');
      const isUmrahCategory = categoryText.includes('umrah');

      // Accounts update (atomic)
      if (transactionType === 'transfer' && hasValidAmount) {
        if (!sourceAccountId || !targetAccountId) {
          throw new Error('Transfer requires both source and target accounts');
        }
        // from --
        const fromRes = await accounts.updateOne(
          { _id: new ObjectId(sourceAccountId) },
          { $inc: { balance: -numericAmount }, $set: { updatedAt: new Date() } },
          { session }
        );
        if (fromRes.matchedCount === 0) throw new Error('Source account not found');

        // to ++
        const toRes = await accounts.updateOne(
          { _id: new ObjectId(targetAccountId) },
          { $inc: { balance: numericAmount }, $set: { updatedAt: new Date() } },
          { session }
        );
        if (toRes.matchedCount === 0) throw new Error('Target account not found');

        updatedSourceAccount = await accounts.findOne({ _id: new ObjectId(sourceAccountId) }, { session });
        updatedTargetAccount = await accounts.findOne({ _id: new ObjectId(targetAccountId) }, { session });
      } else if (transactionType === 'credit' && hasValidAmount) {
        if (!targetAccountId) throw new Error('Credit requires targetAccountId');
        const toRes = await accounts.updateOne(
          { _id: new ObjectId(targetAccountId) },
          { $inc: { balance: numericAmount }, $set: { updatedAt: new Date() } },
          { session }
        );
        if (toRes.matchedCount === 0) throw new Error('Target account not found');
        updatedTargetAccount = await accounts.findOne({ _id: new ObjectId(targetAccountId) }, { session });
      } else if (transactionType === 'debit' && hasValidAmount) {
        if (!sourceAccountId) throw new Error('Debit requires sourceAccountId');
        const fromRes = await accounts.updateOne(
          { _id: new ObjectId(sourceAccountId) },
          { $inc: { balance: -numericAmount }, $set: { updatedAt: new Date() } },
          { session }
        );
        if (fromRes.matchedCount === 0) throw new Error('Source account not found');
        updatedSourceAccount = await accounts.findOne({ _id: new ObjectId(sourceAccountId) }, { session });
      }

      // Party updates (dues/payments)
      // dueDelta: debit => +amount (আমাদের কাছে বেশি দেনা), credit => -amount (আমাদের কাছে দেনা কমলো)
      const dueDelta = hasValidAmount
        ? (transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0))
        : 0;

      if (partyType === 'agent' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ agentId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ agentId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        const doc = await agents.findOne(cond, { session });
        if (doc) {
          const incObj = { totalDue: dueDelta };
          if (hasValidAmount && transactionType === 'credit') {
            incObj.totalDeposit = (incObj.totalDeposit || 0) + numericAmount;
          }
          if (isHajjCategory) {
            // field naming used in UI: hajDue
            incObj.hajDue = (incObj.hajDue || 0) + dueDelta;
          }
          if (isUmrahCategory) {
            incObj.umrahDue = (incObj.umrahDue || 0) + dueDelta;
          }

          await agents.updateOne(
            { _id: doc._id },
            { $inc: incObj, $set: { updatedAt: new Date() } },
            { session }
          );

          // Clamp negatives (e.g., due should not be below 0)
          const after = await agents.findOne({ _id: doc._id }, { session });
          const setClamp = {};
          if ((after.totalDue || 0) < 0) setClamp['totalDue'] = 0;
          if (typeof after.hajDue !== 'undefined' && after.hajDue < 0) setClamp['hajDue'] = 0;
          if (typeof after.umrahDue !== 'undefined' && after.umrahDue < 0) setClamp['umrahDue'] = 0;
          if (Object.keys(setClamp).length) {
            setClamp.updatedAt = new Date();
            await agents.updateOne({ _id: doc._id }, { $set: setClamp }, { session });
          }
          updatedAgent = await agents.findOne({ _id: doc._id }, { session });
        }
      } else if (partyType === 'customer' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        const doc = await customers.findOne(cond, { session });
        if (doc) {
          const incObj = { totalDue: dueDelta };
          // Optional: when customer pays us (credit), track totalPaid
          if (hasValidAmount && transactionType === 'credit') {
            incObj.totalPaid = (incObj.totalPaid || 0) + numericAmount;
          }
          if (isHajjCategory) {
            // customers often store hajjDue
            incObj.hajjDue = (incObj.hajjDue || 0) + dueDelta;
          }
          if (isUmrahCategory) {
            incObj.umrahDue = (incObj.umrahDue || 0) + dueDelta;
          }

          await customers.updateOne(
            { _id: doc._id },
            { $inc: incObj, $set: { updatedAt: new Date() } },
            { session }
          );

          // Clamp negatives
          const after = await customers.findOne({ _id: doc._id }, { session });
          const setClamp = {};
          if ((after.totalDue || 0) < 0) setClamp['totalDue'] = 0;
          if (typeof after.hajjDue !== 'undefined' && after.hajjDue < 0) setClamp['hajjDue'] = 0;
          if (typeof after.umrahDue !== 'undefined' && after.umrahDue < 0) setClamp['umrahDue'] = 0;
          if (Object.keys(setClamp).length) {
            setClamp.updatedAt = new Date();
            await customers.updateOne({ _id: doc._id }, { $set: setClamp }, { session });
          }
          updatedCustomer = await customers.findOne({ _id: doc._id }, { session });

          // Additionally, if this customer also exists in the Haji collection by id/customerId, update paidAmount there on credit
          if (hasValidAmount && transactionType === 'credit') {
            const hajiCond = ObjectId.isValid(tx.partyId)
              ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: { $ne: false } }
              : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: { $ne: false } };
            const hajiDoc = await haji.findOne(hajiCond, { session });
            if (hajiDoc && hajiDoc._id) {
              await haji.updateOne(
                { _id: hajiDoc._id },
                { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
                { session }
              );
              const afterH = await haji.findOne({ _id: hajiDoc._id }, { session });
              const clampH = {};
              if ((afterH.paidAmount || 0) < 0) clampH.paidAmount = 0;
              if (typeof afterH.totalAmount === 'number' && typeof afterH.paidAmount === 'number' && afterH.paidAmount > afterH.totalAmount) {
                clampH.paidAmount = afterH.totalAmount;
              }
              if (Object.keys(clampH).length) {
                clampH.updatedAt = new Date();
                await haji.updateOne({ _id: hajiDoc._id }, { $set: clampH }, { session });
              }
            }

            // Additionally, if this customer also exists in the Umrah collection by id/customerId, update paidAmount there on credit
            // Don't filter by isActive to allow updating deleted/inactive profiles
            const umrahCond = ObjectId.isValid(tx.partyId)
              ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }] }
              : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }] };
            const umrahDoc = await umrah.findOne(umrahCond, { session });
            if (umrahDoc && umrahDoc._id) {
              await umrah.updateOne(
                { _id: umrahDoc._id },
                { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
                { session }
              );
              const afterU = await umrah.findOne({ _id: umrahDoc._id }, { session });
              const clampU = {};
              if ((afterU.paidAmount || 0) < 0) clampU.paidAmount = 0;
              if (typeof afterU.totalAmount === 'number' && typeof afterU.paidAmount === 'number' && afterU.paidAmount > afterU.totalAmount) {
                clampU.paidAmount = afterU.totalAmount;
              }
              if (Object.keys(clampU).length) {
                clampU.updatedAt = new Date();
                await umrah.updateOne({ _id: umrahDoc._id }, { $set: clampU }, { session });
              }
            }
          }
        }
      } else if (partyType === 'vendor' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ vendorId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
          : { $or: [{ vendorId: tx.partyId }, { _id: tx.partyId }], isActive: true };
        const doc = await vendors.findOne(cond, { session });
        if (doc) {
          // Vendor specific logic: debit => vendor ke taka deya (due kombe), credit => vendor theke taka neya (due barbe)
          const vendorDueDelta = hasValidAmount
            ? (transactionType === 'debit' ? -numericAmount : (transactionType === 'credit' ? numericAmount : 0))
            : 0;
          const incObj = { totalDue: vendorDueDelta };
          // Debit সাধারণত ভেন্ডরকে পেমেন্ট—track totalPaid
          if (hasValidAmount && transactionType === 'debit') {
            incObj.totalPaid = (incObj.totalPaid || 0) + numericAmount;
          }
          // Add hajj/umrah due updates for vendor
          if (isHajjCategory) {
            incObj.hajDue = (incObj.hajDue || 0) + vendorDueDelta;
          }
          if (isUmrahCategory) {
            incObj.umrahDue = (incObj.umrahDue || 0) + vendorDueDelta;
          }

          await vendors.updateOne(
            { _id: doc._id },
            { $inc: incObj, $set: { updatedAt: new Date() } },
            { session }
          );

          // Clamp negative dues
          const after = await vendors.findOne({ _id: doc._id }, { session });
          const setClamp = {};
          if ((after.totalDue || 0) < 0) setClamp['totalDue'] = 0;
          if (typeof after.hajDue !== 'undefined' && after.hajDue < 0) setClamp['hajDue'] = 0;
          if (typeof after.umrahDue !== 'undefined' && after.umrahDue < 0) setClamp['umrahDue'] = 0;
          if (Object.keys(setClamp).length) {
            setClamp.updatedAt = new Date();
            await vendors.updateOne({ _id: doc._id }, { $set: setClamp }, { session });
          }
          updatedVendor = await vendors.findOne({ _id: doc._id }, { session });
        }
      }
      // Haji branch: on credit, increase paidAmount
      else if (partyType === 'haji' && tx.partyId) {
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: { $ne: false } }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: { $ne: false } };
        const doc = await haji.findOne(cond, { session });
        if (doc && hasValidAmount && transactionType === 'credit') {
          await haji.updateOne(
            { _id: doc._id },
            { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
            { session }
          );
          const after = await haji.findOne({ _id: doc._id }, { session });
          const clamp = {};
          if ((after.paidAmount || 0) < 0) clamp.paidAmount = 0;
          if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
            clamp.paidAmount = after.totalAmount;
          }
          if (Object.keys(clamp).length) {
            clamp.updatedAt = new Date();
            await haji.updateOne({ _id: doc._id }, { $set: clamp }, { session });
          }
        }
      }
      // Umrah branch: on credit, increase paidAmount
      else if (partyType === 'umrah' && tx.partyId) {
        // Don't filter by isActive to allow updating deleted/inactive profiles
        // This ensures paidAmount is updated correctly even for deleted profiles
        const cond = ObjectId.isValid(tx.partyId)
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }] }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }] };
        const doc = await umrah.findOne(cond, { session });
        if (doc && hasValidAmount && transactionType === 'credit') {
          await umrah.updateOne(
            { _id: doc._id },
            { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
            { session }
          );
          const after = await umrah.findOne({ _id: doc._id }, { session });
          const clamp = {};
          if ((after.paidAmount || 0) < 0) clamp.paidAmount = 0;
          if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
            clamp.paidAmount = after.totalAmount;
          }
          if (Object.keys(clamp).length) {
            clamp.updatedAt = new Date();
            await umrah.updateOne({ _id: doc._id }, { $set: clamp }, { session });
          }
        }
      }

      // Invoice update (if present)
      if (tx.invoiceId && hasValidAmount) {
        const invCond = ObjectId.isValid(tx.invoiceId)
          ? { $or: [{ invoiceId: tx.invoiceId }, { _id: new ObjectId(tx.invoiceId) }], isActive: { $ne: false } }
          : { $or: [{ invoiceId: tx.invoiceId }, { _id: tx.invoiceId }], isActive: { $ne: false } };

        const invoiceDoc = await invoices.findOne(invCond, { session });
        if (invoiceDoc) {
          const addPaid = numericAmount; // both credit and vendor-payment may settle an invoice
          const nextPaid = Math.max(0, (invoiceDoc.paid || 0) + addPaid);
          const nextDue = Math.max(0, Math.max(0, (invoiceDoc.total || 0)) - nextPaid);
          const nextStatus = nextDue <= 0 ? 'Paid' : 'Pending';

          await invoices.updateOne(
            { _id: invoiceDoc._id },
            {
              $set: {
                paid: nextPaid,
                due: nextDue,
                status: nextStatus,
                updatedAt: new Date()
              }
            },
            { session }
          );
          updatedInvoice = await invoices.findOne({ _id: invoiceDoc._id }, { session });
        }
      }

      // Mark transaction completed now
      await transactions.updateOne(
        { _id: tx._id, status: { $ne: 'completed' } },
        { $set: { status: 'completed', completedAt: new Date(), updatedAt: new Date() } },
        { session }
      );
      const updatedTx = await transactions.findOne({ _id: tx._id }, { session });

      await session.commitTransaction();
      return res.json({
        success: true,
        transaction: updatedTx,
        agent: updatedAgent || null,
        customer: updatedCustomer || null,
        vendor: updatedVendor || null,
        invoice: updatedInvoice || null,
        sourceAccount: updatedSourceAccount || null,
        targetAccount: updatedTargetAccount || null
      });
    } catch (err) {
      await session.abortTransaction();
      throw err;
    }
  } catch (error) {
    console.error('Complete transaction error:', error);
    res.status(500).json({ success: false, message: error.message || 'Failed to complete transaction' });
  } finally {
    if (session) session.endSession();
  }
});


// // MongoDB Setup
const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASSWORD}@cluster0.unn2dmm.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0`;
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});



// Helper: Validate date format
const isValidDate = (dateString) => {
  const date = new Date(dateString);
  return date instanceof Date && !isNaN(date) && dateString.match(/^\d{4}-\d{2}-\d{2}$/);
};

// Helper: Generate unique ID for user
const generateUniqueId = async (db, branchCode) => {
  const counterCollection = db.collection("counters");

  // Simple approach: find current counter and increment
  let counter = await counterCollection.findOne({ branchCode });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ branchCode, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { branchCode },
    { $set: { sequence: newSequence } }
  );

  // Format: DH-0001, BOG-0001, etc.
  return `${branchCode}-${String(newSequence).padStart(4, '0')}`;
};

// Helper: Generate unique Customer ID
const generateCustomerId = async (db, customerType) => {
  const counterCollection = db.collection("counters");
  const customerTypesCollection = db.collection("customerTypes");

  // Get customer type details from database
  const typeDetails = await customerTypesCollection.findOne({
    value: customerType.toLowerCase(),
    isActive: true
  });

  if (!typeDetails) {
    throw new Error(`Customer type '${customerType}' not found`);
  }

  // Get current date in DDMMYY format (as requested)
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);

  // Create counter key for customer type and date
  const counterKey = `customer_${customerType}_${dateStr}`;

  // Find or create counter
  let counter = await counterCollection.findOne({ counterKey });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  // Format: PREFIX + DDMMYY + 00001 (e.g., HAJ05092500001)
  const prefix = typeDetails.prefix; // Use prefix from database
  const serial = String(newSequence).padStart(5, '0');

  return `${prefix}${dateStr}${serial}`;
};

// Helper: Generate unique Transaction ID
const generateTransactionId = async (db, branchCode) => {
  const counterCollection = db.collection("counters");

  // Get current date in DDMMYY format
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);

  // Create counter key for transaction and date
  const counterKey = `transaction_${branchCode}_${dateStr}`;

  // Find or create counter
  let counter = await counterCollection.findOne({ counterKey });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  // Format: TXN + H + DDMMYY + 00001 (e.g., TXNH2508290001)
  const serial = String(newSequence).padStart(4, '0');

  return `TXN${branchCode}${dateStr}${serial}`;
};

// Helper: Generate unique Order ID
const generateOrderId = async (db, branchCode) => {
  const counterCollection = db.collection("counters");

  // Get current date in DDMMYY format
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);

  // Create counter key for order and date
  const counterKey = `order_${branchCode}_${dateStr}`;

  // Find or create counter
  let counter = await counterCollection.findOne({ counterKey });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  // Format: ORD + branchCode + DDMMYY + 00001 (e.g., ORDDH2508290001)
  const serial = String(newSequence).padStart(4, '0');

  return `ORD${branchCode}${dateStr}${serial}`;
};

// Helper: Generate unique Loan ID
const generateLoanId = async (db, branchCode, loanType = 'giving') => {
  const counterCollection = db.collection("counters");

  // Get current date in DDMMYY format
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);

  // Create counter key for loan type and date
  const counterKey = `loan_${loanType}_${branchCode}_${dateStr}`;

  // Find or create counter
  let counter = await counterCollection.findOne({ counterKey });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  // Format: LOAN + type prefix (G/R) + branchCode + DDMMYY + 0001
  // giving: LOANGDH2508290001, receiving: LOANRDH2508290001
  const typePrefix = loanType === 'giving' ? 'G' : 'R';
  const serial = String(newSequence).padStart(4, '0');

  return `LOAN${typePrefix}${branchCode}${dateStr}${serial}`;
};

// Helper: Generate unique Vendor ID
const generateVendorId = async (db) => {
  const counterCollection = db.collection("counters");

  // Create counter key for vendor
  const counterKey = `vendor`;

  // Find or create counter
  let counter = await counterCollection.findOne({ counterKey });

  if (!counter) {
    // Create new counter starting from 0
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  // Increment sequence
  const newSequence = counter.sequence + 1;

  // Update counter
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  // Format: VN + 00001 (e.g., VN00001)
  const serial = String(newSequence).padStart(5, '0');

  return `VN${serial}`;
};

// Initialize default customer types
const initializeDefaultCustomerTypes = async (db, customerTypes) => {
  const defaultTypes = [
    {
      value: 'haj',
      label: 'হাজ্জ',
      icon: 'Home',
      prefix: 'HAJ'
    },
    {
      value: 'umrah',
      label: 'ওমরাহ',
      icon: 'Plane',
      prefix: 'UMR'
    }
  ];

  for (const type of defaultTypes) {
    await customerTypes.updateOne(
      { value: type.value },
      {
        $setOnInsert: {
          ...type,
          isActive: true,
          createdAt: new Date(),
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );
  }

  console.log("✅ Default customer types initialized successfully");
};


// Initialize default branches
const initializeDefaultBranches = async (db, branches, counters) => {
  const defaultBranches = [
    { branchId: 'main', branchName: 'Main Office', branchLocation: 'Dhaka, Bangladesh', branchCode: 'DH' },
    { branchId: 'bogra', branchName: 'Bogra Branch', branchLocation: 'Bogra, Bangladesh', branchCode: 'BOG' },
    { branchId: 'dupchanchia', branchName: 'Dupchanchia Branch', branchLocation: 'Dupchanchia, Bangladesh', branchCode: 'DUP' },
    { branchId: 'chittagong', branchName: 'Chittagong Branch', branchLocation: 'Chittagong, Bangladesh', branchCode: 'CTG' },
    { branchId: 'sylhet', branchName: 'Sylhet Branch', branchLocation: 'Sylhet, Bangladesh', branchCode: 'SYL' },
    { branchId: 'rajshahi', branchName: 'Rajshahi Branch', branchLocation: 'Rajshahi, Bangladesh', branchCode: 'RAJ' },
    { branchId: 'khulna', branchName: 'Khulna Branch', branchLocation: 'Khulna, Bangladesh', branchCode: 'KHU' },
    { branchId: 'barisal', branchName: 'Barisal Branch', branchLocation: 'Barisal, Bangladesh', branchCode: 'BAR' },
    { branchId: 'rangpur', branchName: 'Rangpur Branch', branchLocation: 'Rangpur, Bangladesh', branchCode: 'RAN' },
    { branchId: 'mymensingh', branchName: 'Mymensingh Branch', branchLocation: 'Mymensingh, Bangladesh', branchCode: 'MYM' }
  ];

  for (const branch of defaultBranches) {
    await branches.updateOne(
      { branchId: branch.branchId },
      {
        $setOnInsert: {
          ...branch,
          isActive: true,
          createdAt: new Date(),
          updatedAt: new Date()
        }
      },
      { upsert: true }
    );

    // Initialize counter for each branch (start from 0, first user will get 1)
    await counters.updateOne(
      { branchCode: branch.branchCode },
      { $setOnInsert: { branchCode: branch.branchCode, sequence: 0 } },
      { upsert: true }
    );
  }

};

// Global variables for database collections
let db, users, branches, counters, customers, customerTypes, services, sales, vendors, orders, bankAccounts, categories, agents, hrManagement, haji, umrah, agentPackages, packages, transactions, invoices, accounts, vendorBills, loans;

// Initialize database connection
async function initializeDatabase() {
  try {
    await client.connect();
    console.log("✅ MongoDB connected");

    db = client.db("erpDashboard");
    users = db.collection("users");
    branches = db.collection("branches");
    counters = db.collection("counters");
    customers = db.collection("customers");
    customerTypes = db.collection("customerTypes");
    services = db.collection("services");
    sales = db.collection("sales");
    vendors = db.collection("vendors");
    orders = db.collection("orders");
    bankAccounts = db.collection("bankAccounts");
    categories = db.collection("categories");
    agents = db.collection("agents");
    hrManagement = db.collection("hr_management");
    haji = db.collection("haji");
    umrah = db.collection("umrah");
    agentPackages = db.collection("agent_packages");
    packages = db.collection("packages");
    transactions = db.collection("transactions");
    invoices = db.collection("invoices");
    accounts = db.collection("accounts");
    vendorBills = db.collection("vendorBills");
    loans = db.collection("loans");




    // Initialize default branches
    await initializeDefaultBranches(db, branches, counters);

    // Initialize default customer types
    await initializeDefaultCustomerTypes(db, customerTypes);
  } catch (error) {
    console.error("❌ Database initialization error:", error);
    throw error;
  }
}

// Initialize database connection immediately
let isInitialized = false;
let dbConnectionError = null;

async function ensureDatabaseInitialized() {
  if (!isInitialized) {
    try {
      await initializeDatabase();
      isInitialized = true;
      console.log("✅ Database initialized successfully");
    } catch (error) {
      console.error("❌ Database initialization failed:", error.message);
      dbConnectionError = error;
      // Don't throw the error, just log it
    }
  }
}

// Initialize database on startup
ensureDatabaseInitialized().catch(console.error);

// ==================== ROOT ENDPOINT ====================
app.get("/", (req, res) => {
  // Return HTML page with a link to the dashboard
  res.send(`
    <!DOCTYPE html>
    <html lang="bn">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>ERP Dashboard API</title>
        <style>
            body {
                font-family: 'Hind Siliguri', Arial, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                margin: 0;
                padding: 0;
                min-height: 100vh;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .container {
                background: white;
                padding: 3rem;
                border-radius: 20px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                text-align: center;
                max-width: 500px;
                width: 90%;
            }
            h1 {
                color: #333;
                margin-bottom: 1rem;
                font-size: 2.5rem;
            }
            .status {
                color: #28a745;
                font-size: 1.2rem;
                margin-bottom: 2rem;
                font-weight: 600;
            }
            .dashboard-link {
                display: inline-block;
                background: linear-gradient(45deg, #667eea, #764ba2);
                color: white;
                text-decoration: none;
                padding: 15px 30px;
                border-radius: 50px;
                font-size: 1.1rem;
                font-weight: 600;
                transition: transform 0.3s ease, box-shadow 0.3s ease;
                margin: 10px;
            }
            .dashboard-link:hover {
                transform: translateY(-3px);
                box-shadow: 0 10px 25px rgba(102, 126, 234, 0.4);
            }
            .api-info {
                margin-top: 2rem;
                padding: 1rem;
                background: #f8f9fa;
                border-radius: 10px;
                color: #666;
            }
            .version {
                font-size: 0.9rem;
                color: #999;
                margin-top: 1rem;
            }
        </style>
        <link href="https://fonts.googleapis.com/css2?family=Hind+Siliguri:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    </head>
    <body>
        <div class="container">
            <h1>🚀 ERP Dashboard API</h1>
            <div class="status">✅ API সফলভাবে চলছে!</div>
            
            <a href="https://erp-dashboard1.netlify.app" class="dashboard-link" target="_blank">
                📊 ড্যাশবোর্ডে যান
            </a>
            
            <div class="api-info">
                <p><strong>API Status:</strong> সক্রিয়</p>
                <p><strong>Version:</strong> 1.0.0</p>
                <p><strong>Database:</strong> MongoDB</p>
                <p><strong>Authentication:</strong> Firebase + JWT</p>
            </div>
            
            <div class="version">
                ERP Dashboard Backend API - Powered by Node.js & Express
            </div>
        </div>
    </body>
    </html>
  `);
});




// ==================== AUTH ROUTES ====================
app.post("/api/auth/login", async (req, res) => {
  try {
    const { email, firebaseUid, displayName, branchId } = req.body;

    if (!email || !firebaseUid) {
      return res.status(400).json({
        error: 'Email and firebaseUid are required.'
      });
    }

    // Check if user already exists
    let user = await users.findOne({
      email: email.toLowerCase(),
      firebaseUid,
      isActive: true
    });

    // If user doesn't exist, create new user (auto signup)
    if (!user) {
      if (!displayName || !branchId) {
        return res.status(400).json({
          error: 'For new users, displayName and branchId are required.'
        });
      }

      // Get branch information
      const branch = await branches.findOne({ branchId, isActive: true });
      if (!branch) {
        return res.status(400).json({
          error: 'Invalid branch ID.'
        });
      }

      // Generate unique ID for the user based on branch
      const uniqueId = await generateUniqueId(db, branch.branchCode);

      // Create new user with default role 'user'
      const newUser = {
        uniqueId,
        displayName,
        email: email.toLowerCase(),
        branchId,
        branchName: branch.branchName,
        branchLocation: branch.branchLocation,
        firebaseUid,
        role: 'user',
        isActive: true,
        createdAt: new Date(),
        updatedAt: new Date()
      };

      const result = await users.insertOne(newUser);
      user = { ...newUser, _id: result.insertedId };

      console.log(`✅ New user created: ${uniqueId} (${displayName})`);
    }

    // Generate JWT token
    const token = jwt.sign(
      {
        sub: user._id.toString(),
        uniqueId: user.uniqueId,
        email: user.email,
        role: user.role,
        branchId: user.branchId
      },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRES || '7d' }
    );

    res.json({
      success: true,
      message: user.uniqueId ? 'User created and logged in successfully' : 'Login successful',
      token,
      user: {
        uniqueId: user.uniqueId,
        displayName: user.displayName,
        email: user.email,
        role: user.role,
        branchId: user.branchId,
        branchName: user.branchName
      }
    });

  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      error: 'Internal server error during login.'
    });
  }
});

// ==================== BRANCH ROUTES ====================
app.get("/api/branches/active", async (req, res) => {
  try {
    const activeBranches = await branches.find({ isActive: true })
      .project({ branchId: 1, branchName: 1, branchLocation: 1, branchCode: 1 })
      .sort({ branchName: 1 })
      .toArray();

    res.json({
      success: true,
      branches: activeBranches
    });
  } catch (error) {
    console.error('Get active branches error:', error);
    res.status(500).json({
      error: 'Internal server error while fetching branches.'
    });
  }
});

// ==================== JWT TOKEN GENERATION ====================
app.post("/jwt", async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) {
      return res.status(400).send({ error: true, message: "Email is required" });
    }

    // Fetch user from DB
    const user = await users.findOne({ email: email.toLowerCase() });
    if (!user) {
      return res.status(404).send({ error: true, message: "User not found" });
    }

    // Sign token including role and uniqueId
    const token = jwt.sign(
      {
        email,
        role: user.role || "user",
        uniqueId: user.uniqueId,
        branchId: user.branchId
      },
      process.env.JWT_SECRET,
      { expiresIn: "7d" }
    );

    res.send({ token });
  } catch (error) {
    console.error('JWT generation error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// ==================== USER ROUTES ====================
app.post("/users", async (req, res) => {
  try {
    const { email, displayName, branchId, firebaseUid, role = "user" } = req.body;

    if (!email || !displayName || !branchId || !firebaseUid) {
      return res.status(400).send({
        error: true,
        message: "Email, displayName, branchId, and firebaseUid are required"
      });
    }

    // Check if user already exists
    const exists = await users.findOne({ email: email.toLowerCase() });
    if (exists) {
      return res.status(400).send({ error: true, message: "User already exists" });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).send({ error: true, message: "Invalid branch ID" });
    }

    // Generate unique ID for the user
    const uniqueId = await generateUniqueId(db, branch.branchCode);

    // Create new user
    const newUser = {
      uniqueId,
      displayName,
      email: email.toLowerCase(),
      branchId,
      branchName: branch.branchName,
      branchLocation: branch.branchLocation,
      firebaseUid,
      role,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await users.insertOne(newUser);

    res.status(201).send({
      success: true,
      message: "User created successfully",
      user: {
        _id: result.insertedId,
        uniqueId: newUser.uniqueId,
        displayName: newUser.displayName,
        email: newUser.email,
        role: newUser.role,
        branchId: newUser.branchId,
        branchName: newUser.branchName
      }
    });
  } catch (error) {
    console.error('Create user error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

app.get("/users", async (req, res) => {
  try {
    // Check if database is connected
    if (!users) {
      return res.status(503).json({
        error: true,
        message: "Database not connected. Please try again later."
      });
    }

    const allUsers = await users.find({ isActive: true })
      .project({ firebaseUid: 0 })
      .sort({ createdAt: -1 })
      .toArray();
    res.send(allUsers);
  } catch (error) {
    console.error('Get users error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

app.patch("/users/role/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { role } = req.body;

    console.log('Role update request:', { id, role, body: req.body });

    if (!role) {
      return res.status(400).send({ error: true, message: "Role is required" });
    }

    // Validate role
    const validRoles = ['super admin', 'admin', 'account', 'reservation', 'user'];
    if (!validRoles.includes(role.toLowerCase())) {
      return res.status(400).send({
        error: true,
        message: `Invalid role. Must be one of: ${validRoles.join(', ')}`
      });
    }

    // Find user by uniqueId first (since you're using DH-0002 format)
    let user = await users.findOne({ uniqueId: id, isActive: true });

    // If not found by uniqueId, try by _id
    if (!user && ObjectId.isValid(id)) {
      user = await users.findOne({ _id: new ObjectId(id), isActive: true });
    }

    if (!user) {
      return res.status(404).send({
        error: true,
        message: `User not found with ID: ${id}`
      });
    }

    console.log('Found user:', { uniqueId: user.uniqueId, currentRole: user.role, newRole: role });

    const result = await users.updateOne(
      { _id: user._id },
      { $set: { role: role.toLowerCase(), updatedAt: new Date() } }
    );

    if (result.modifiedCount === 0) {
      return res.status(400).send({
        error: true,
        message: "No changes made. Role might be the same."
      });
    }

    res.send({
      success: true,
      message: `User role updated successfully from ${user.role} to ${role}`,
      user: {
        uniqueId: user.uniqueId,
        displayName: user.displayName,
        email: user.email,
        oldRole: user.role,
        newRole: role.toLowerCase()
      },
      result
    });
  } catch (error) {
    console.error('Update user role error:', error);

    // More specific error messages
    if (error.name === 'ValidationError') {
      return res.status(400).send({
        error: true,
        message: "Validation error: " + error.message
      });
    }

    if (error.name === 'CastError') {
      return res.status(400).send({
        error: true,
        message: "Invalid ID format"
      });
    }

    res.status(500).send({
      error: true,
      message: "Internal server error during role update",
      details: error.message
    });
  }
});



app.get("/users/role/:email", async (req, res) => {
  try {
    const user = await users.findOne({
      email: req.params.email.toLowerCase(),
      isActive: true
    });
    res.send({ role: user?.role || null });
  } catch (error) {
    console.error('Get user role error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

app.get("/users/profile/:email", async (req, res) => {
  try {
    const user = await users.findOne({
      email: req.params.email.toLowerCase(),
      isActive: true
    });

    if (!user) {
      return res.status(404).send({ error: true, message: "User not found" });
    }

    res.send({
      uniqueId: user.uniqueId,
      displayName: user.displayName,
      email: user.email,
      role: user.role,
      branchId: user.branchId,
      branchName: user.branchName,
      photoURL: user.photoURL || null,
      createdAt: user.createdAt
    });
  } catch (error) {
    console.error('Get user profile error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

app.patch("/users/profile/:email", async (req, res) => {
  try {
    const email = req.params.email.toLowerCase();
    const updateData = req.body;

    // Validate required fields
    if (!email) {
      return res.status(400).json({
        error: true,
        message: "Email parameter is required"
      });
    }

    // Check if user exists
    const existingUser = await users.findOne({
      email: email,
      isActive: true
    });

    if (!existingUser) {
      return res.status(404).json({
        error: true,
        message: "User not found"
      });
    }

    // Prepare update data
    const allowedFields = ['name', 'displayName', 'phone', 'address', 'department', 'photoURL'];
    const filteredUpdateData = {};

    // Only allow specific fields to be updated
    allowedFields.forEach(field => {
      if (updateData[field] !== undefined) {
        filteredUpdateData[field] = updateData[field];
      }
    });

    // If name is provided, also update displayName for consistency
    if (filteredUpdateData.name && !filteredUpdateData.displayName) {
      filteredUpdateData.displayName = filteredUpdateData.name;
    }

    // Add update timestamp
    filteredUpdateData.updatedAt = new Date();

    // Validate phone number format if provided
    if (filteredUpdateData.phone && !/^01[3-9]\d{8}$/.test(filteredUpdateData.phone)) {
      return res.status(400).json({
        error: true,
        message: "Invalid phone number format. Please use 01XXXXXXXXX format"
      });
    }

    // Update user profile
    const result = await users.updateOne(
      { email: email, isActive: true },
      { $set: filteredUpdateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        error: true,
        message: "User not found"
      });
    }

    // Get updated user data
    const updatedUser = await users.findOne({
      email: email,
      isActive: true
    });

    res.json({
      success: true,
      message: "User profile updated successfully",
      modifiedCount: result.modifiedCount,
      user: {
        uniqueId: updatedUser.uniqueId,
        displayName: updatedUser.displayName,
        name: updatedUser.displayName,
        email: updatedUser.email,
        phone: updatedUser.phone || '',
        address: updatedUser.address || '',
        department: updatedUser.department || '',
        role: updatedUser.role,
        branchId: updatedUser.branchId,
        branchName: updatedUser.branchName,
        photoURL: updatedUser.photoURL || null,
        createdAt: updatedUser.createdAt,
        updatedAt: updatedUser.updatedAt,
        isActive: updatedUser.isActive
      }
    });
  } catch (error) {
    console.error("Failed to update user profile:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while updating profile"
    });
  }
});

// ==================== CUSTOMER TYPE ROUTES ====================

// Get all customer types
app.get("/customer-types", async (req, res) => {
  try {
    const allTypes = await customerTypes.find({ isActive: true })
      .sort({ createdAt: 1 })
      .toArray();

    res.json({
      success: true,
      customerTypes: allTypes
    });
  } catch (error) {
    console.error('Get customer types error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching customer types"
    });
  }
});

// Create new customer type
app.post("/customer-types", async (req, res) => {
  try {
    const { value, label, icon, prefix } = req.body;

    // Validation
    if (!value || !label || !prefix) {
      return res.status(400).json({
        error: true,
        message: "Value, label, and prefix are required"
      });
    }

    // Validate value format (should be lowercase, alphanumeric with hyphens/underscores)
    if (!/^[a-z0-9_-]+$/.test(value)) {
      return res.status(400).json({
        error: true,
        message: "Value must contain only lowercase letters, numbers, hyphens, and underscores"
      });
    }

    // Validate prefix format (should be uppercase letters and numbers)
    if (!/^[A-Z0-9]+$/.test(prefix)) {
      return res.status(400).json({
        error: true,
        message: "Prefix must contain only uppercase letters and numbers"
      });
    }

    // Check if customer type already exists
    const existingType = await customerTypes.findOne({
      value: value.toLowerCase(),
      isActive: true
    });

    if (existingType) {
      return res.status(400).json({
        error: true,
        message: "Customer type with this value already exists"
      });
    }

    // Create new customer type
    const newCustomerType = {
      value: value.toLowerCase(),
      label,
      icon: icon || 'Home',
      prefix: prefix.toUpperCase(), // Store as uppercase
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await customerTypes.insertOne(newCustomerType);

    res.status(201).json({
      success: true,
      message: "Customer type created successfully",
      customerType: {
        _id: result.insertedId,
        ...newCustomerType
      }
    });

  } catch (error) {
    console.error('Create customer type error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while creating customer type"
    });
  }
});

// Update customer type
app.patch("/customer-types/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        error: true,
        message: "Invalid customer type ID format"
      });
    }

    // Remove fields that shouldn't be updated
    delete updateData._id;
    delete updateData.createdAt;
    updateData.updatedAt = new Date();

    // Check if value is being updated and if it already exists
    if (updateData.value) {
      // Validate value format
      if (!/^[a-z0-9_-]+$/.test(updateData.value)) {
        return res.status(400).json({
          error: true,
          message: "Value must contain only lowercase letters, numbers, hyphens, and underscores"
        });
      }

      const existingType = await customerTypes.findOne({
        value: updateData.value.toLowerCase(),
        _id: { $ne: new ObjectId(id) },
        isActive: true
      });

      if (existingType) {
        return res.status(400).json({
          error: true,
          message: "Customer type with this value already exists"
        });
      }
      updateData.value = updateData.value.toLowerCase();
    }

    // Validate prefix format if being updated
    if (updateData.prefix) {
      if (!/^[A-Z0-9]+$/.test(updateData.prefix)) {
        return res.status(400).json({
          error: true,
          message: "Prefix must contain only uppercase letters and numbers"
        });
      }
      updateData.prefix = updateData.prefix.toUpperCase();
    }

    const result = await customerTypes.updateOne(
      { _id: new ObjectId(id), isActive: true },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        error: true,
        message: "Customer type not found"
      });
    }

    res.json({
      success: true,
      message: "Customer type updated successfully",
      modifiedCount: result.modifiedCount
    });

  } catch (error) {
    console.error('Update customer type error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while updating customer type"
    });
  }
});

// Delete customer type (soft delete)
app.delete("/customer-types/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        error: true,
        message: "Invalid customer type ID format"
      });
    }

    // First get the customer type to check its value
    const customerTypeToDelete = await customerTypes.findOne({
      _id: new ObjectId(id),
      isActive: true
    });

    if (!customerTypeToDelete) {
      return res.status(404).json({
        error: true,
        message: "Customer type not found"
      });
    }

    // Check if any customers are using this type
    const customersUsingType = await customers.countDocuments({
      customerType: customerTypeToDelete.value,
      isActive: true
    });

    if (customersUsingType > 0) {
      return res.status(400).json({
        error: true,
        message: `Cannot delete customer type. ${customersUsingType} customers are using this type.`
      });
    }

    const result = await customerTypes.updateOne(
      { _id: new ObjectId(id), isActive: true },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    res.json({
      success: true,
      message: "Customer type deleted successfully"
    });

  } catch (error) {
    console.error('Delete customer type error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting customer type"
    });
  }
});

// ==================== CUSTOMER ROUTES ====================

// Create new customer
app.post("/customers", async (req, res) => {
  try {
    const {
      name,
      mobile,
      email,
      address,
      division,
      district,
      upazila,
      whatsappNo,
      customerType,
      customerImage,
      passportNumber,
      issueDate,
      expiryDate,
      dateOfBirth,
      nidNumber,
      notes,
      referenceBy,
      referenceCustomerId,
      postCode,
      // Newly supported personal fields
      firstName,
      lastName,
      fatherName,
      motherName,
      spouseName,
      occupation,
      nationality,
      gender,
      maritalStatus,
      // Newly supported passport fields
      passportType,
      // Service linkage fields
      serviceType,
      serviceStatus,
      // Payment/financial fields
      totalAmount,
      paidAmount,
      paymentMethod,
      paymentStatus,
      // Package information object
      packageInfo,
      // Explicit isActive toggle
      isActive
    } = req.body;

    // Validation
    if (!name || !mobile || !address || !division || !district || !upazila || !customerType) {
      return res.status(400).send({
        error: true,
        message: "Name, mobile, address, division, district, upazila, and customerType are required"
      });
    }

    // Validate customer type exists in database
    const validCustomerType = await customerTypes.findOne({
      value: customerType.toLowerCase(),
      isActive: true
    });

    if (!validCustomerType) {
      return res.status(400).send({
        error: true,
        message: `Invalid customer type '${customerType}'. Please select a valid customer type.`
      });
    }

    // Validate passport fields if provided
    if (issueDate && !isValidDate(issueDate)) {
      return res.status(400).send({
        error: true,
        message: "Invalid issue date format. Please use YYYY-MM-DD format"
      });
    }

    if (expiryDate && !isValidDate(expiryDate)) {
      return res.status(400).send({
        error: true,
        message: "Invalid expiry date format. Please use YYYY-MM-DD format"
      });
    }

    if (dateOfBirth && !isValidDate(dateOfBirth)) {
      return res.status(400).send({
        error: true,
        message: "Invalid date of birth format. Please use YYYY-MM-DD format"
      });
    }

    // Check if expiry date is after issue date
    if (issueDate && expiryDate && new Date(expiryDate) <= new Date(issueDate)) {
      return res.status(400).send({
        error: true,
        message: "Expiry date must be after issue date"
      });
    }

    // Check if mobile already exists
    const existingCustomer = await customers.findOne({
      mobile: mobile,
      isActive: true
    });

    if (existingCustomer) {
      return res.status(400).send({
        error: true,
        message: "Customer with this mobile number already exists"
      });
    }

    // Generate unique customer ID
    const customerId = await generateCustomerId(db, customerType);


    // Process image data - Extract just the URL string
    let imageUrl = null;

    // If customerImage is a string (direct URL), use it
    if (typeof customerImage === 'string') {
      imageUrl = customerImage;
    }
    // If customerImage is an object with cloudinaryUrl, extract the URL
    else if (customerImage && typeof customerImage === 'object' && customerImage.cloudinaryUrl) {
      imageUrl = customerImage.cloudinaryUrl;
    }
    // If customerImage is an object with downloadURL, use that
    else if (customerImage && typeof customerImage === 'object' && customerImage.downloadURL) {
      imageUrl = customerImage.downloadURL;
    }

    // Create customer object
    const newCustomer = {
      customerId,
      name,
      // Additional name breakdown (optional)
      firstName: firstName || null,
      lastName: lastName || null,
      mobile,
      email: email || null,
      address,
      division,
      district,
      upazila,
      postCode: postCode || null,
      whatsappNo: whatsappNo || null,
      customerType,
      customerImage: imageUrl, // Just the image URL
      // Passport information fields
      passportNumber: passportNumber || null,
      passportType: passportType || null,
      issueDate: issueDate || null,
      expiryDate: expiryDate || null,
      dateOfBirth: dateOfBirth || null,
      nidNumber: nidNumber || null,
      nationality: nationality || null,
      gender: gender || null,
      maritalStatus: maritalStatus || null,
      // Family and personal details
      fatherName: fatherName || null,
      motherName: motherName || null,
      spouseName: spouseName || null,
      occupation: occupation || null,
      // Additional fields
      notes: notes || null,
      referenceBy: referenceBy || null,
      referenceCustomerId: referenceCustomerId || null,
      // Service linkage
      serviceType: serviceType || null,
      serviceStatus: serviceStatus || null,
      // Financial fields
      totalAmount: typeof totalAmount === 'number' ? totalAmount : null,
      paidAmount: typeof paidAmount === 'number' ? paidAmount : null,
      paymentMethod: paymentMethod || null,
      paymentStatus: paymentStatus || null,
      // Package info (store as provided if object)
      packageInfo: packageInfo && typeof packageInfo === 'object' ? packageInfo : null,
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: typeof isActive === 'boolean' ? isActive : true
    };

    const result = await customers.insertOne(newCustomer);

    res.status(201).send({
      success: true,
      message: "Customer created successfully",
      customer: {
        _id: result.insertedId,
        customerId: newCustomer.customerId,
        name: newCustomer.name,
        mobile: newCustomer.mobile,
        email: newCustomer.email,
        address: newCustomer.address,
        division: newCustomer.division,
        district: newCustomer.district,
        upazila: newCustomer.upazila,
        postCode: newCustomer.postCode,
        whatsappNo: newCustomer.whatsappNo,
        customerType: newCustomer.customerType,
        customerImage: newCustomer.customerImage,
        firstName: newCustomer.firstName,
        lastName: newCustomer.lastName,
        fatherName: newCustomer.fatherName,
        motherName: newCustomer.motherName,
        spouseName: newCustomer.spouseName,
        occupation: newCustomer.occupation,
        passportNumber: newCustomer.passportNumber,
        passportType: newCustomer.passportType,
        issueDate: newCustomer.issueDate,
        expiryDate: newCustomer.expiryDate,
        dateOfBirth: newCustomer.dateOfBirth,
        nidNumber: newCustomer.nidNumber,
        nationality: newCustomer.nationality,
        gender: newCustomer.gender,
        maritalStatus: newCustomer.maritalStatus,
        notes: newCustomer.notes,
        referenceBy: newCustomer.referenceBy,
        referenceCustomerId: newCustomer.referenceCustomerId,
        customerImage: newCustomer.customerImage,
        serviceType: newCustomer.serviceType,
        serviceStatus: newCustomer.serviceStatus,
        totalAmount: newCustomer.totalAmount,
        paidAmount: newCustomer.paidAmount,
        paymentMethod: newCustomer.paymentMethod,
        paymentStatus: newCustomer.paymentStatus,
        packageInfo: newCustomer.packageInfo,
        createdAt: newCustomer.createdAt
      }
    });

  } catch (error) {
    console.error('Create customer error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// Get all customers
app.get("/customers", async (req, res) => {
  try {
    // Check if database is connected
    if (!customers) {
      return res.status(503).json({
        error: true,
        message: "Database not connected. Please try again later."
      });
    }

    const { customerType, division, district, upazila, search, passportNumber, nidNumber, expiringSoon, serviceType, serviceStatus, paymentStatus } = req.query;

    let filter = { isActive: true };

    // Apply filters
    if (customerType) filter.customerType = customerType;
    if (division) filter.division = division;
    if (district) filter.district = district;
    if (upazila) filter.upazila = upazila;
    if (passportNumber) filter.passportNumber = { $regex: passportNumber, $options: 'i' };
    if (nidNumber) filter.nidNumber = { $regex: nidNumber, $options: 'i' };
    if (serviceType) filter.serviceType = serviceType;
    if (serviceStatus) filter.serviceStatus = serviceStatus;
    if (paymentStatus) filter.paymentStatus = paymentStatus;

    // Filter customers with expiring passports (within next 30 days)
    if (expiringSoon === 'true') {
      const thirtyDaysFromNow = new Date();
      thirtyDaysFromNow.setDate(thirtyDaysFromNow.getDate() + 30);
      filter.expiryDate = {
        $gte: new Date().toISOString().split('T')[0],
        $lte: thirtyDaysFromNow.toISOString().split('T')[0]
      };
    }
    if (search) {
      filter.$or = [
        { name: { $regex: search, $options: 'i' } },
        { mobile: { $regex: search, $options: 'i' } },
        { customerId: { $regex: search, $options: 'i' } },
        { passportNumber: { $regex: search, $options: 'i' } },
        { nidNumber: { $regex: search, $options: 'i' } }
      ];
    }

    const allCustomers = await customers.find(filter)
      .sort({ createdAt: -1 })
      .toArray();

    res.send({
      success: true,
      count: allCustomers.length,
      customers: allCustomers
    });
  } catch (error) {
    console.error('Get customers error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// Search customers for transaction form
app.get("/customers/search", async (req, res) => {
  try {
    const { q } = req.query;

    if (!q || q.trim().length < 2) {
      return res.json({
        success: true,
        customers: []
      });
    }

    const searchTerm = q.trim();
    const searchRegex = new RegExp(searchTerm, 'i');

    const searchResults = await customers.find({
      isActive: true,
      $or: [
        { name: searchRegex },
        { mobile: searchRegex },
        { email: searchRegex },
        { customerId: searchRegex },
        { passportNumber: searchRegex },
        { nidNumber: searchRegex }
      ]
    })
      .project({
        customerId: 1,
        name: 1,
        mobile: 1,
        email: 1,
        customerType: 1,
        address: 1,
        division: 1,
        district: 1
      })
      .sort({ name: 1 })
      .limit(20)
      .toArray();

    res.json({
      success: true,
      customers: searchResults
    });
  } catch (error) {
    console.error('Search customers error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while searching customers"
    });
  }
});

// Get customer by ID (supports customerId or Mongo _id)
app.get("/customers/:customerId", async (req, res) => {
  try {
    const { customerId } = req.params;

    // Try find by customerId first
    let customer = await customers.findOne({
      customerId: customerId,
      isActive: true
    });

    // If not found and looks like a valid ObjectId, try by _id
    if (!customer && ObjectId.isValid(customerId)) {
      customer = await customers.findOne({
        _id: new ObjectId(customerId),
        isActive: true
      });
    }

    if (!customer) {
      return res.status(404).send({
        error: true,
        message: "Customer not found"
      });
    }

    res.send({
      success: true,
      customer: customer
    });
  } catch (error) {
    console.error('Get customer error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// Update customer
app.patch("/customers/:customerId", async (req, res) => {
  try {
    const { customerId } = req.params;
    const updateData = { ...req.body };

    // Remove fields that shouldn't be updated
    delete updateData.customerId;
    delete updateData.createdAt;
    updateData.updatedAt = new Date();

    // Normalize customerImage if provided (like create route)
    if (Object.prototype.hasOwnProperty.call(updateData, 'customerImage')) {
      let imageUrl = null;
      const incoming = updateData.customerImage;
      if (typeof incoming === 'string') {
        imageUrl = incoming;
      } else if (incoming && typeof incoming === 'object' && incoming.cloudinaryUrl) {
        imageUrl = incoming.cloudinaryUrl;
      } else if (incoming && typeof incoming === 'object' && incoming.downloadURL) {
        imageUrl = incoming.downloadURL;
      }
      updateData.customerImage = imageUrl;
    }

    // Check if mobile is being updated and if it already exists
    if (updateData.mobile) {
      const existingCustomer = await customers.findOne({
        mobile: updateData.mobile,
        customerId: { $ne: customerId },
        isActive: true
      });

      if (existingCustomer) {
        return res.status(400).send({
          error: true,
          message: "Customer with this mobile number already exists"
        });
      }
    }

    // Validate passport fields if being updated
    if (updateData.issueDate && !isValidDate(updateData.issueDate)) {
      return res.status(400).send({
        error: true,
        message: "Invalid issue date format. Please use YYYY-MM-DD format"
      });
    }

    if (updateData.expiryDate && !isValidDate(updateData.expiryDate)) {
      return res.status(400).send({
        error: true,
        message: "Invalid expiry date format. Please use YYYY-MM-DD format"
      });
    }

    if (updateData.dateOfBirth && !isValidDate(updateData.dateOfBirth)) {
      return res.status(400).send({
        error: true,
        message: "Invalid date of birth format. Please use YYYY-MM-DD format"
      });
    }

    // Check if expiry date is after issue date
    if (updateData.issueDate && updateData.expiryDate && new Date(updateData.expiryDate) <= new Date(updateData.issueDate)) {
      return res.status(400).send({
        error: true,
        message: "Expiry date must be after issue date"
      });
    }

    // Validate payment fields if provided
    if (Object.prototype.hasOwnProperty.call(updateData, 'totalAmount')) {
      if (updateData.totalAmount !== null && updateData.totalAmount !== undefined && typeof updateData.totalAmount !== 'number') {
        return res.status(400).send({ error: true, message: "totalAmount must be a number" });
      }
    }
    if (Object.prototype.hasOwnProperty.call(updateData, 'paidAmount')) {
      if (updateData.paidAmount !== null && updateData.paidAmount !== undefined && typeof updateData.paidAmount !== 'number') {
        return res.status(400).send({ error: true, message: "paidAmount must be a number" });
      }
    }
    if (typeof updateData.totalAmount === 'number' && typeof updateData.paidAmount === 'number') {
      if (updateData.paidAmount > updateData.totalAmount) {
        return res.status(400).send({ error: true, message: "paidAmount cannot exceed totalAmount" });
      }
    }

    // Validate enums if provided
    if (updateData.paymentStatus && !['pending', 'partial', 'paid'].includes(updateData.paymentStatus)) {
      return res.status(400).send({ error: true, message: "Invalid paymentStatus value" });
    }
    if (updateData.serviceStatus && !['pending', 'confirmed', 'cancelled', 'in_progress', 'completed'].includes(updateData.serviceStatus)) {
      return res.status(400).send({ error: true, message: "Invalid serviceStatus value" });
    }

    const result = await customers.updateOne(
      { customerId: customerId, isActive: true },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).send({
        error: true,
        message: "Customer not found"
      });
    }

    // Return updated customer
    const updatedCustomer = await customers.findOne({ customerId, isActive: true });
    res.send({
      success: true,
      message: "Customer updated successfully",
      customer: updatedCustomer
    });
  } catch (error) {
    console.error('Update customer error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// Delete customer (soft delete)
app.delete("/customers/:customerId", async (req, res) => {
  try {
    const { customerId } = req.params;

    const result = await customers.updateOne(
      { customerId: customerId, isActive: true },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).send({
        error: true,
        message: "Customer not found"
      });
    }

    res.send({
      success: true,
      message: "Customer deleted successfully"
    });
  } catch (error) {
    console.error('Delete customer error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// Get customer statistics
app.get("/customers/stats/overview", async (req, res) => {
  try {
    const totalCustomers = await customers.countDocuments({ isActive: true });
    const hajCustomers = await customers.countDocuments({ customerType: 'Haj', isActive: true });
    const umrahCustomers = await customers.countDocuments({ customerType: 'Umrah', isActive: true });

    // Get today's customers
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const todayCustomers = await customers.countDocuments({
      createdAt: { $gte: today },
      isActive: true
    });

    // Get this month's customers
    const thisMonth = new Date();
    thisMonth.setDate(1);
    thisMonth.setHours(0, 0, 0, 0);
    const thisMonthCustomers = await customers.countDocuments({
      createdAt: { $gte: thisMonth },
      isActive: true
    });

    // Get passport statistics
    const customersWithPassport = await customers.countDocuments({
      passportNumber: { $exists: true, $ne: null },
      isActive: true
    });

    const customersWithNID = await customers.countDocuments({
      nidNumber: { $exists: true, $ne: null },
      isActive: true
    });

    // Get customers with expiring passports (within next 30 days)
    const thirtyDaysFromNow = new Date();
    thirtyDaysFromNow.setDate(thirtyDaysFromNow.getDate() + 30);
    const expiringPassports = await customers.countDocuments({
      expiryDate: {
        $gte: new Date().toISOString().split('T')[0],
        $lte: thirtyDaysFromNow.toISOString().split('T')[0]
      },
      isActive: true
    });

    // Get customers with images
    const customersWithImage = await customers.countDocuments({
      customerImage: { $exists: true, $ne: null },
      isActive: true
    });

    res.send({
      success: true,
      stats: {
        total: totalCustomers,
        haj: hajCustomers,
        umrah: umrahCustomers,
        today: todayCustomers,
        thisMonth: thisMonthCustomers,
        withPassport: customersWithPassport,
        withNID: customersWithNID,
        expiringPassports: expiringPassports,
        withImage: customersWithImage
      }
    });
  } catch (error) {
    console.error('Get customer stats error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});

// ==================== PASSPORT ROUTES ====================

// Get passport statistics
app.get("/customers/passport/stats", async (req, res) => {
  try {
    const { days = 30 } = req.query;
    const daysFromNow = new Date();
    daysFromNow.setDate(daysFromNow.getDate() + parseInt(days));

    const expiringPassports = await customers.find({
      expiryDate: {
        $gte: new Date().toISOString().split('T')[0],
        $lte: daysFromNow.toISOString().split('T')[0]
      },
      isActive: true
    }).sort({ expiryDate: 1 }).toArray();

    const expiredPassports = await customers.find({
      expiryDate: {
        $lt: new Date().toISOString().split('T')[0]
      },
      isActive: true
    }).sort({ expiryDate: -1 }).toArray();

    res.send({
      success: true,
      stats: {
        expiringWithinDays: parseInt(days),
        expiringCount: expiringPassports.length,
        expiredCount: expiredPassports.length,
        expiringPassports,
        expiredPassports
      }
    });
  } catch (error) {
    console.error('Get passport stats error:', error);
    res.status(500).send({ error: true, message: "Internal server error" });
  }
});


// Add new service type
app.post('/api/services', async (req, res) => {
  try {
    const { value, label } = req.body;
    if (!value || !label) return res.status(400).json({ error: 'Value and Label required' });

    const exists = await services.findOne({ value });
    if (exists) return res.status(400).json({ error: 'Service type already exists' });

    await services.insertOne({ value, label, statuses: [] });
    res.status(201).json({ message: 'Service type added' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to add service type' });
  }
});
// Get all service types
app.get('/api/services', async (req, res) => {
  try {
    const allServices = await services.find().toArray();
    res.json({ services: allServices });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch services' });
  }
});

// Delete service type
app.delete('/api/services/:serviceValue', async (req, res) => {
  try {
    const { serviceValue } = req.params;
    await services.deleteOne({ value: serviceValue });
    res.json({ message: 'Service type deleted' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete service type' });
  }
});

// ------------------- Service Status Routes -------------------

// Get statuses for a service
app.get('/api/services/:serviceValue/statuses', async (req, res) => {
  try {
    const { serviceValue } = req.params;
    const service = await services.findOne({ value: serviceValue });
    if (!service) return res.status(404).json({ error: 'Service not found' });

    res.json({ statuses: service.statuses });
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch statuses' });
  }
});

// Add a status to a service
app.post('/api/services/:serviceValue/statuses', async (req, res) => {
  try {
    const { serviceValue } = req.params;
    const { value, label } = req.body;

    if (!value || !label) return res.status(400).json({ error: 'Value and Label required' });

    const service = await services.findOne({ value: serviceValue });
    if (!service) return res.status(404).json({ error: 'Service not found' });

    const exists = service.statuses.find(st => st.value === value);
    if (exists) return res.status(400).json({ error: 'Status already exists' });

    await services.updateOne(
      { value: serviceValue },
      { $push: { statuses: { value, label } } }
    );

    res.status(201).json({ message: 'Status added' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to add status' });
  }
});

// Delete a status
app.delete('/api/services/:serviceValue/statuses/:statusValue', async (req, res) => {
  try {
    const { serviceValue, statusValue } = req.params;
    const service = await services.findOne({ value: serviceValue });
    if (!service) return res.status(404).json({ error: 'Service not found' });

    await services.updateOne(
      { value: serviceValue },
      { $pull: { statuses: { value: statusValue } } }
    );

    res.json({ message: 'Status deleted' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete status' });
  }
});

// Fallback delete using body
app.delete('/api/services/:serviceValue/statuses', async (req, res) => {
  try {
    const { serviceValue } = req.params;
    const { value } = req.body;
    const service = await services.findOne({ value: serviceValue });
    if (!service) return res.status(404).json({ error: 'Service not found' });

    await services.updateOne(
      { value: serviceValue },
      { $pull: { statuses: { value } } }
    );

    res.json({ message: 'Status deleted' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete status' });
  }
});

// ==================== CATEGORY ROUTES ====================
// Normalize a category document for response
const normalizeCategoryDoc = (doc) => ({
  id: String(doc._id || doc.id || doc.categoryId || ""),
  name: doc.name || "",
  icon: doc.icon || "",
  description: doc.description || "",
  subCategories: Array.isArray(doc.subCategories) ? doc.subCategories.map((s) => ({
    id: String(s._id || s.id || s.subCategoryId || ""),
    name: s.name || "",
    icon: s.icon || "",
    description: s.description || ""
  })) : []
});

// GET all categories
app.get("/api/categories", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await categories.find({}).toArray();
    return res.json(list.map(normalizeCategoryDoc));
  } catch (err) {
    console.error("/api/categories GET error:", err);
    return res.status(500).json({ error: true, message: "Failed to load categories" });
  }
});

// CREATE category
app.post("/api/categories", async (req, res) => {
  try {
    const { name, icon = "", description = "", subCategories = [] } = req.body || {};
    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Category name is required" });
    }
    const doc = {
      name: String(name).trim(),
      icon: String(icon || ""),
      description: String(description || ""),
      subCategories: Array.isArray(subCategories) ? subCategories.map((s) => ({
        id: s.id || s._id || undefined,
        name: s.name || "",
        icon: s.icon || "",
        description: s.description || ""
      })) : []
    };
    const result = await categories.insertOne(doc);
    const created = await categories.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeCategoryDoc(created));
  } catch (err) {
    console.error("/api/categories POST error:", err);
    return res.status(500).json({ error: true, message: "Failed to create category" });
  }
});

// UPDATE category
app.put("/api/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, icon = "", description = "", subCategories } = req.body || {};
    const update = {};
    if (typeof name !== 'undefined') update.name = String(name).trim();
    if (typeof icon !== 'undefined') update.icon = String(icon || "");
    if (typeof description !== 'undefined') update.description = String(description || "");
    if (typeof subCategories !== 'undefined') {
      update.subCategories = Array.isArray(subCategories) ? subCategories.map((s) => ({
        id: s.id || s._id || undefined,
        name: s.name || "",
        icon: s.icon || "",
        description: s.description || ""
      })) : [];
    }
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const filter = { _id: new ObjectId(id) };
    const result = await categories.findOneAndUpdate(filter, { $set: update }, { returnDocument: 'after' });
    const updatedDoc = result && (result.value || result); // support different driver return shapes
    if (!updatedDoc) return res.status(404).json({ error: true, message: "Category not found" });
    return res.json(normalizeCategoryDoc(updatedDoc));
  } catch (err) {
    console.error("/api/categories/:id PUT error:", err);
    return res.status(500).json({ error: true, message: "Failed to update category" });
  }
});

// DELETE category
app.delete("/api/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const filter = { _id: new ObjectId(id) };
    const result = await categories.deleteOne(filter);
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Category not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("/api/categories/:id DELETE error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete category" });
  }
});

// ==================== SUBCATEGORY ROUTES ====================

// CREATE subcategory under a category
app.post("/api/categories/:id/subcategories", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, icon = "", description = "" } = req.body || {};
    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Subcategory name is required" });
    }
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const filter = { _id: new ObjectId(id) };
    const sub = {
      id: new ObjectId().toString(),
      name: String(name).trim(),
      icon: String(icon || ""),
      description: String(description || "")
    };
    const result = await categories.updateOne(filter, { $push: { subCategories: sub } });
    if (result.matchedCount === 0) return res.status(404).json({ error: true, message: "Category not found" });
    const updated = await categories.findOne(filter);
    return res.status(201).json(normalizeCategoryDoc(updated));
  } catch (err) {
    console.error("POST subcategory error:", err);
    return res.status(500).json({ error: true, message: "Failed to add subcategory" });
  }
});

// PATCH subcategory field(s)
app.patch("/api/categories/:id/subcategories/:subId", async (req, res) => {
  try {
    const { id, subId } = req.params;
    const allowed = ["name", "icon", "description"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: true, message: "No valid fields to update" });
    }
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const filter = { _id: new ObjectId(id) };
    const cat = await categories.findOne(filter);
    if (!cat) return res.status(404).json({ error: true, message: "Category not found" });
    const nextSubs = (cat.subCategories || []).map((s) => {
      if (String(s.id || s._id) === String(subId)) {
        return { ...s, ...updates };
      }
      return s;
    });
    await categories.updateOne(filter, { $set: { subCategories: nextSubs } });
    const updated = await categories.findOne(filter);
    return res.json(normalizeCategoryDoc(updated));
  } catch (err) {
    console.error("PATCH subcategory error:", err);
    return res.status(500).json({ error: true, message: "Failed to update subcategory" });
  }
});

// DELETE subcategory
app.delete("/api/categories/:id/subcategories/:subId", async (req, res) => {
  try {
    const { id, subId } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const filter = { _id: new ObjectId(id) };
    const result = await categories.updateOne(filter, { $pull: { subCategories: { id: String(subId) } } });
    if (result.matchedCount === 0) return res.status(404).json({ error: true, message: "Category not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE subcategory error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete subcategory" });
  }
});

// ✅ GET: Categories wise Credit and Debit Summary
app.get("/api/categories-summary", async (req, res) => {
  try {
    const { fromDate, toDate } = req.query || {};
    
    // Build date filter for transactions
    const dateFilter = {};
    if (fromDate || toDate) {
      dateFilter.date = {};
      if (fromDate) dateFilter.date.$gte = new Date(fromDate);
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
        }
        dateFilter.date.$lte = end;
      }
    }
    
    // Get all categories
    const allCategories = await categories.find({}).toArray();
    
    // Get all transactions grouped by serviceCategory
    const allTransactions = await transactions.find({
      ...dateFilter,
      isActive: { $ne: false }
    }).toArray();
    
    // Initialize summary object with all categories
    const categorySummary = {};
    
    // Initialize all categories with 0 values
    allCategories.forEach(category => {
      categorySummary[category.name] = {
        categoryId: String(category._id),
        categoryName: category.name,
        totalCredit: 0,
        totalDebit: 0,
        netAmount: 0
      };
    });
    
    // Aggregate transactions by category
    allTransactions.forEach(tx => {
      const categoryName = tx.serviceCategory || tx.category || 'Others';
      
      if (!categorySummary[categoryName]) {
        categorySummary[categoryName] = {
          categoryId: null,
          categoryName: categoryName,
          totalCredit: 0,
          totalDebit: 0,
          netAmount: 0
        };
      }
      
      if (tx.transactionType === 'credit') {
        categorySummary[categoryName].totalCredit += tx.amount || 0;
      } else if (tx.transactionType === 'debit') {
        categorySummary[categoryName].totalDebit += tx.amount || 0;
      }
      
      // Calculate net amount
      categorySummary[categoryName].netAmount = 
        categorySummary[categoryName].totalCredit - categorySummary[categoryName].totalDebit;
    });
    
    // Convert object to array and format numbers
    const summaryArray = Object.values(categorySummary).map(item => ({
      categoryId: item.categoryId,
      categoryName: item.categoryName,
      totalCredit: parseFloat(item.totalCredit.toFixed(2)),
      totalDebit: parseFloat(item.totalDebit.toFixed(2)),
      netAmount: parseFloat(item.netAmount.toFixed(2))
    }));
    
    // Calculate grand totals
    const grandTotal = {
      totalCredit: parseFloat(summaryArray.reduce((sum, item) => sum + item.totalCredit, 0).toFixed(2)),
      totalDebit: parseFloat(summaryArray.reduce((sum, item) => sum + item.totalDebit, 0).toFixed(2)),
      netAmount: parseFloat(summaryArray.reduce((sum, item) => sum + item.netAmount, 0).toFixed(2))
    };
    
    res.json({
      success: true,
      data: summaryArray,
      grandTotal,
      period: {
        fromDate: fromDate || null,
        toDate: toDate || null
      }
    });
    
  } catch (err) {
    console.error("/api/categories-summary GET error:", err);
    res.status(500).json({ error: true, message: "Failed to load categories summary" });
  }
});





// Sale And Invoice 

// ✅ POST: Get Sale from saleData
app.post("/sales", async (req, res) => {
  try {
    const { saleData } = req.body;
    const { saleId } = saleData;

    const sale = await sales.findOne({ saleId, isActive: true });
    if (!sale) {
      return res.status(404).json({ error: true, message: "Sale not found" });
    }

    res.json({ success: true, sale });
  } catch (error) {
    console.error("Sale fetch error:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching sale",
    });
  }
});

// ✅ GET: Get Sale by saleId
app.get("/sales/:saleId", async (req, res) => {
  try {
    const { saleId } = req.params;

    const sale = await sales.findOne({ saleId, isActive: true });
    if (!sale) {
      return res.status(404).json({ error: true, message: "Sale not found" });
    }

    res.json({ success: true, sale });
  } catch (error) {
    console.error("Get sale error:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching sale",
    });
  }
});


//     // ✅ POST: Add new vendor
// app.post("/vendors", async (req, res) => {
//   try {
//     const {
//       tradeName,
//       tradeLocation,
//       ownerName,
//       contactNo,
//       dob,
//       nid,
//       passport
//     } = req.body;

//     if (!tradeName || !tradeLocation || !ownerName || !contactNo) {
//       return res.status(400).json({
//         error: true,
//         message: "Trade Name, Location, Owner Name & Contact No are required",
//       });
//     }

//     // Normalize and validate contact number
//     let normalizedContact = String(contactNo || '').trim();
//     const contactDigits = normalizedContact.replace(/\D/g, '');
//     if (contactDigits.startsWith('8801') && contactDigits.length >= 13) {
//       normalizedContact = '0' + contactDigits.slice(3, 13);
//     } else if (contactDigits.startsWith('01') && contactDigits.length >= 11) {
//       normalizedContact = contactDigits.slice(0, 11);
//     } else {
//       normalizedContact = contactDigits;
//     }
//     if (!/^01[3-9]\d{8}$/.test(normalizedContact)) {
//       return res.status(400).json({
//         error: true,
//         message: "Invalid contact number format. Please use 01XXXXXXXXX format"
//       });
//     }

//     // Handle DOB: treat empty string as null; validate if provided
//     let dobToStore = (dob === '' ? null : (dob || null));
//     if (dobToStore !== null) {
//       if (!/^\d{4}-\d{2}-\d{2}$/.test(String(dobToStore)) || Number.isNaN(new Date(dobToStore).getTime())) {
//         return res.status(400).json({ error: true, message: 'Invalid date format. Please use YYYY-MM-DD format' });
//       }
//     }

//     // Generate unique vendor ID
//     const vendorId = await generateVendorId(db);

//     const newVendor = {
//       vendorId: vendorId,
//       tradeName: tradeName.trim(),
//       tradeLocation: tradeLocation.trim(),
//       ownerName: ownerName.trim(),
//       contactNo: normalizedContact,
//       dob: dobToStore,
//       nid: nid?.trim() || "",
//       passport: passport?.trim() || "",
//       isActive: true,
//       createdAt: new Date(),
//     };

//     const result = await vendors.insertOne(newVendor);

//     res.status(201).json({
//       success: true,
//       message: "Vendor added successfully",
//       vendorId: result.insertedId,
//       vendorUniqueId: vendorId,
//       vendor: { _id: result.insertedId, ...newVendor },
//     });
//   } catch (error) {
//     console.error("Error adding vendor:", error);
//     res.status(500).json({
//       error: true,
//       message: "Internal server error while adding vendor",
//     });
//   }
// });

// Vendor add and list

// ✅ POST: Add new vendor
app.post("/vendors", async (req, res) => {
  try {
    const {
      tradeName,
      tradeLocation,
      ownerName,
      contactNo,
      dob,
      nid,
      passport
    } = req.body;

    if (!tradeName || !tradeLocation || !ownerName || !contactNo) {
      return res.status(400).json({
        error: true,
        message: "Trade Name, Location, Owner Name & Contact No are required",
      });
    }

    // Generate unique vendor ID
    const vendorId = await generateVendorId(db);

    const newVendor = {
      vendorId: vendorId,
      tradeName: tradeName.trim(),
      tradeLocation: tradeLocation.trim(),
      ownerName: ownerName.trim(),
      contactNo: contactNo.trim(),
      dob: dob || null,
      nid: nid?.trim() || "",
      passport: passport?.trim() || "",
      isActive: true,
      createdAt: new Date(),
    };

    const result = await vendors.insertOne(newVendor);

    res.status(201).json({
      success: true,
      message: "Vendor added successfully",
      vendorId: result.insertedId,
      vendorUniqueId: vendorId,
    });
  } catch (error) {
    console.error("Error adding vendor:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while adding vendor",
    });
  }
});

// ✅ GET: All active vendors
app.get("/vendors", async (req, res) => {
  try {
    const allVendors = await vendors.find({ isActive: true }).toArray();

    // Initialize due amounts if missing (migration for old vendors)
    for (const vendor of allVendors) {
      if (vendor.totalDue === undefined || vendor.hajDue === undefined || vendor.umrahDue === undefined || vendor.totalPaid === undefined) {
        console.log('🔄 Migrating vendor to add due amounts:', vendor._id);
        const updateDoc = {};
        if (vendor.totalDue === undefined) updateDoc.totalDue = 0;
        if (vendor.hajDue === undefined) updateDoc.hajDue = 0;
        if (vendor.umrahDue === undefined) updateDoc.umrahDue = 0;
        if (vendor.totalPaid === undefined) updateDoc.totalPaid = 0;
        updateDoc.updatedAt = new Date();

        await vendors.updateOne(
          { _id: vendor._id },
          { $set: updateDoc }
        );

        Object.assign(vendor, updateDoc);
        console.log('✅ Vendor migrated successfully');
      }
    }

    res.json({ success: true, vendors: allVendors });
  } catch (error) {
    console.error("Error fetching vendors:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendors",
    });
  }
});


// ✅ GET: Single vendor by ID
app.get("/vendors/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Check if valid MongoDB ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid vendor ID" });
    }

    const vendor = await vendors.findOne({
      _id: new ObjectId(id),
      isActive: true,
    });

    if (!vendor) {
      return res.status(404).json({ error: true, message: "Vendor not found" });
    }

    // Initialize due amounts if missing (migration for old vendors)
    if (vendor.totalDue === undefined || vendor.hajDue === undefined || vendor.umrahDue === undefined || vendor.totalPaid === undefined) {
      console.log('🔄 Migrating vendor to add due amounts:', vendor._id);
      const updateDoc = {};
      if (vendor.totalDue === undefined) updateDoc.totalDue = 0;
      if (vendor.hajDue === undefined) updateDoc.hajDue = 0;
      if (vendor.umrahDue === undefined) updateDoc.umrahDue = 0;
      if (vendor.totalPaid === undefined) updateDoc.totalPaid = 0;
      updateDoc.updatedAt = new Date();

      await vendors.updateOne(
        { _id: new ObjectId(id) },
        { $set: updateDoc }
      );

      Object.assign(vendor, updateDoc);
      console.log('✅ Vendor migrated successfully');
    }

    res.json({ success: true, vendor });
  } catch (error) {
    console.error("Error fetching vendor:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor",
    });
  }
});

// ✅ PATCH: Update vendor information
app.patch("/vendors/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Check if valid MongoDB ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid vendor ID" });
    }

    // Check if vendor exists
    const existingVendor = await vendors.findOne({
      _id: new ObjectId(id),
      isActive: true,
    });

    if (!existingVendor) {
      return res.status(404).json({ error: true, message: "Vendor not found" });
    }

    // Prepare update data - only allow specific fields to be updated
    const allowedFields = ['tradeName', 'tradeLocation', 'ownerName', 'contactNo', 'dob', 'nid', 'passport'];
    const filteredUpdateData = {};

    // Only allow specific fields to be updated
    allowedFields.forEach(field => {
      if (updateData[field] !== undefined) {
        // Trim string fields
        if (typeof updateData[field] === 'string') {
          filteredUpdateData[field] = updateData[field].trim();
        } else {
          filteredUpdateData[field] = updateData[field];
        }
      }
    });

    // Validate required fields if they are being updated
    if (filteredUpdateData.tradeName && !filteredUpdateData.tradeName) {
      return res.status(400).json({
        error: true,
        message: "Trade Name cannot be empty"
      });
    }

    if (filteredUpdateData.tradeLocation && !filteredUpdateData.tradeLocation) {
      return res.status(400).json({
        error: true,
        message: "Trade Location cannot be empty"
      });
    }

    if (filteredUpdateData.ownerName && !filteredUpdateData.ownerName) {
      return res.status(400).json({
        error: true,
        message: "Owner Name cannot be empty"
      });
    }

    if (filteredUpdateData.contactNo && !filteredUpdateData.contactNo) {
      return res.status(400).json({
        error: true,
        message: "Contact Number cannot be empty"
      });
    }

    // Validate contact number format if being updated
    if (filteredUpdateData.contactNo && !/^01[3-9]\d{8}$/.test(filteredUpdateData.contactNo)) {
      return res.status(400).json({
        error: true,
        message: "Invalid contact number format. Please use 01XXXXXXXXX format"
      });
    }

    // Validate date of birth format if being updated
    if (filteredUpdateData.dob && filteredUpdateData.dob !== null && !isValidDate(filteredUpdateData.dob)) {
      return res.status(400).json({
        error: true,
        message: "Invalid date format. Please use YYYY-MM-DD format"
      });
    }

    // Check if contact number already exists for another vendor
    if (filteredUpdateData.contactNo) {
      const existingVendorWithContact = await vendors.findOne({
        contactNo: filteredUpdateData.contactNo,
        _id: { $ne: new ObjectId(id) },
        isActive: true
      });

      if (existingVendorWithContact) {
        return res.status(400).json({
          error: true,
          message: "Vendor with this contact number already exists"
        });
      }
    }

    // Add update timestamp
    filteredUpdateData.updatedAt = new Date();

    // Remove fields that shouldn't be updated
    delete filteredUpdateData._id;
    delete filteredUpdateData.createdAt;
    delete filteredUpdateData.isActive;

    // Update vendor
    const result = await vendors.updateOne(
      { _id: new ObjectId(id), isActive: true },
      { $set: filteredUpdateData }
    );

    if (result.modifiedCount === 0) {
      return res.status(400).json({
        error: true,
        message: "No changes made or vendor not found"
      });
    }

    // Get updated vendor data
    const updatedVendor = await vendors.findOne({
      _id: new ObjectId(id),
      isActive: true,
    });

    res.json({
      success: true,
      message: "Vendor information updated successfully",
      modifiedCount: result.modifiedCount,
      vendor: updatedVendor
    });

  } catch (error) {
    console.error("Error updating vendor:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while updating vendor",
    });
  }
});

//✅ DELETE (soft delete)
app.delete("/vendors/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid vendor ID" });
    }

    const result = await vendors.updateOne(
      { _id: new ObjectId(id) },
      { $set: { isActive: false } }
    );

    if (result.modifiedCount === 0) {
      return res.status(404).json({ error: true, message: "Vendor not found" });
    }

    res.json({ success: true, message: "Vendor deleted successfully" });
  } catch (error) {
    console.error("Error deleting vendor:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting vendor",
    });
  }
});


// ✅ GET: Vendor statistics overview
app.get("/vendors/stats/overview", async (req, res) => {
  try {
    // Totals
    const totalVendors = await vendors.countDocuments({ isActive: true });

    // Today
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);
    const todayEnd = new Date();
    todayEnd.setHours(23, 59, 59, 999);

    const todayCount = await vendors.countDocuments({
      isActive: true,
      createdAt: { $gte: todayStart, $lte: todayEnd },
    });

    // This month
    const monthStart = new Date();
    monthStart.setDate(1);
    monthStart.setHours(0, 0, 0, 0);
    const thisMonthCount = await vendors.countDocuments({
      isActive: true,
      createdAt: { $gte: monthStart },
    });

    // With NID / Passport
    const withNID = await vendors.countDocuments({ isActive: true, nid: { $exists: true, $ne: "" } });
    const withPassport = await vendors.countDocuments({ isActive: true, passport: { $exists: true, $ne: "" } });

    // By tradeLocation
    const byLocation = await vendors.aggregate([
      { $match: { isActive: true } },
      { $group: { _id: "$tradeLocation", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();

    res.json({
      success: true,
      stats: {
        total: totalVendors,
        today: todayCount,
        thisMonth: thisMonthCount,
        withNID,
        withPassport,
        byLocation
      }
    });
  } catch (error) {
    console.error("Error fetching vendor statistics:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor statistics",
    });
  }
});

// ✅ GET: Vendor statistics data (detailed analytics)
app.get("/vendors/stats/data", async (req, res) => {
  try {
    const { period = 'month', location } = req.query;

    let dateFilter = {};
    const now = new Date();

    // Set date range based on period
    switch (period) {
      case 'week':
        const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
        dateFilter = { $gte: weekAgo };
        break;
      case 'month':
        const monthAgo = new Date(now.getFullYear(), now.getMonth(), 1);
        dateFilter = { $gte: monthAgo };
        break;
      case 'quarter':
        const quarterStart = new Date(now.getFullYear(), Math.floor(now.getMonth() / 3) * 3, 1);
        dateFilter = { $gte: quarterStart };
        break;
      case 'year':
        const yearStart = new Date(now.getFullYear(), 0, 1);
        dateFilter = { $gte: yearStart };
        break;
      default:
        dateFilter = { $gte: new Date(now.getFullYear(), now.getMonth(), 1) };
    }

    // Base match filter
    let matchFilter = { isActive: true, createdAt: dateFilter };

    // Add location filter if specified
    if (location) {
      matchFilter.tradeLocation = { $regex: location, $options: 'i' };
    }

    // Vendor registration trends over time
    const registrationTrends = await vendors.aggregate([
      { $match: matchFilter },
      {
        $group: {
          _id: {
            year: { $year: "$createdAt" },
            month: { $month: "$createdAt" },
            day: period === 'week' ? { $dayOfMonth: "$createdAt" } : null
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }
    ]).toArray();

    // Vendors by location (top locations)
    const vendorsByLocation = await vendors.aggregate([
      { $match: matchFilter },
      { $group: { _id: "$tradeLocation", count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 10 }
    ]).toArray();

    // Vendor demographics (with/without documents)
    const documentStats = await vendors.aggregate([
      { $match: matchFilter },
      {
        $group: {
          _id: null,
          withNID: {
            $sum: { $cond: [{ $and: [{ $ne: ["$nid", ""] }, { $ne: ["$nid", null] }] }, 1, 0] }
          },
          withPassport: {
            $sum: { $cond: [{ $and: [{ $ne: ["$passport", ""] }, { $ne: ["$passport", null] }] }, 1, 0] }
          },
          withoutDocuments: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $or: [{ $eq: ["$nid", ""] }, { $eq: ["$nid", null] }] },
                    { $or: [{ $eq: ["$passport", ""] }, { $eq: ["$passport", null] }] }
                  ]
                },
                1, 0
              ]
            }
          },
          total: { $sum: 1 }
        }
      }
    ]).toArray();

    // Recent vendor activity (last 30 days)
    const thirtyDaysAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const recentVendors = await vendors.find({
      isActive: true,
      createdAt: { $gte: thirtyDaysAgo }
    }).sort({ createdAt: -1 }).limit(10).toArray();

    // Vendor growth rate
    const currentPeriod = await vendors.countDocuments(matchFilter);

    let previousPeriodFilter = {};
    const currentDate = new Date();
    switch (period) {
      case 'week':
        const twoWeeksAgo = new Date(currentDate.getTime() - 14 * 24 * 60 * 60 * 1000);
        const oneWeekAgo = new Date(currentDate.getTime() - 7 * 24 * 60 * 60 * 1000);
        previousPeriodFilter = { isActive: true, createdAt: { $gte: twoWeeksAgo, $lt: oneWeekAgo } };
        break;
      case 'month':
        const lastMonth = new Date(currentDate.getFullYear(), currentDate.getMonth() - 1, 1);
        const currentMonth = new Date(currentDate.getFullYear(), currentDate.getMonth(), 1);
        previousPeriodFilter = { isActive: true, createdAt: { $gte: lastMonth, $lt: currentMonth } };
        break;
      default:
        previousPeriodFilter = { isActive: true, createdAt: { $gte: new Date(currentDate.getFullYear(), currentDate.getMonth() - 1, 1), $lt: new Date(currentDate.getFullYear(), currentDate.getMonth(), 1) } };
    }

    const previousPeriod = await vendors.countDocuments(previousPeriodFilter);
    const growthRate = previousPeriod > 0 ? ((currentPeriod - previousPeriod) / previousPeriod * 100) : 0;

    res.json({
      success: true,
      data: {
        period,
        totalVendors: currentPeriod,
        growthRate: Math.round(growthRate * 100) / 100,
        registrationTrends,
        vendorsByLocation,
        documentStats: documentStats[0] || { withNID: 0, withPassport: 0, withoutDocuments: 0, total: 0 },
        recentVendors,
        summary: {
          period,
          total: currentPeriod,
          previousPeriod,
          growthRate: Math.round(growthRate * 100) / 100,
          topLocation: vendorsByLocation[0] || null
        }
      }
    });
  } catch (error) {
    console.error("Error fetching vendor statistics data:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor statistics data",
    });
  }
});

app.get('/vendors/:id/financials', async (req, res) => {
  const { id } = req.params;
  const query = ObjectId.isValid(id)
    ? { _id: new ObjectId(id), isActive: true }
    : { vendorId: id, isActive: true };

  const vendor = await vendors.findOne(query, {
    projection: { totalAmount: 1, paidAmount: 1, outstandingAmount: 1, lastPaymentDate: 1 }
  });
  if (!vendor) return res.status(404).json({ success: false, message: 'Vendor not found' });

  res.json({
    success: true,
    financials: {
      totalAmount: vendor.totalAmount || 0,
      paidAmount: vendor.paidAmount || 0,
      outstandingAmount: vendor.outstandingAmount || 0,
      lastPaymentDate: vendor.lastPaymentDate || null
    }
  });
});

// ==================== VENDOR BILLS ROUTES ====================

// ✅ POST: Create new vendor bill
app.post("/vendors/bills", async (req, res) => {
  try {
    const billData = req.body;
    const {
      vendorId,
      vendorName,
      billType,
      billDate,
      billNumber,
      totalAmount,
      amount,
      paymentMethod,
      paymentStatus,
      dueDate,
      createdBy,
      branchId,
      createdAt,
      ...otherFields
    } = billData;

    // Validation
    if (!vendorId || !billType || !billDate || !totalAmount) {
      return res.status(400).json({
        error: true,
        message: "Vendor ID, Bill Type, Bill Date, and Total Amount are required"
      });
    }

    // Validate amount
    const parsedTotalAmount = parseFloat(totalAmount);
    if (isNaN(parsedTotalAmount) || parsedTotalAmount <= 0) {
      return res.status(400).json({
        error: true,
        message: "Total Amount must be greater than 0"
      });
    }

    // Check if vendor exists
    let vendor;
    if (ObjectId.isValid(vendorId)) {
      vendor = await vendors.findOne({ _id: new ObjectId(vendorId), isActive: true });
    } else {
      const normalized = String(vendorId).trim().toUpperCase();
      vendor = await vendors.findOne({ vendorId: normalized, isActive: true });
    }

    if (!vendor) {
      return res.status(404).json({
        error: true,
        message: "Vendor not found"
      });
    }

    // Create bill document
    const newBill = {
      vendorId: vendor.vendorId || vendorId,
      vendorName: vendor.tradeName || vendorName,
      billType: billType.trim(),
      billDate: new Date(billDate),
      billNumber: billNumber || `${billType}-${Date.now()}`,
      totalAmount: parsedTotalAmount,
      amount: parseFloat(amount) || parsedTotalAmount,
      paymentMethod: paymentMethod || '',
      paymentStatus: paymentStatus || 'pending',
      dueDate: dueDate ? new Date(dueDate) : null,
      createdBy: createdBy || 'unknown',
      branchId: branchId || 'main_branch',
      createdAt: new Date(createdAt || Date.now()),
      updatedAt: new Date(),
      isActive: true,
      // Include all other fields from the request
      ...otherFields
    };

    // Insert bill into vendorBills collection
    const result = await vendorBills.insertOne(newBill);

    // Update vendor financials based on bill type
    const isHajj = billType.toLowerCase().includes('hajj') || billType.toLowerCase().includes('haj');
    const isUmrah = billType.toLowerCase().includes('umrah');

    const vendorUpdate = { $set: { updatedAt: new Date(), lastBillDate: new Date(billDate) } };
    
    // Increase totalDue
    vendorUpdate.$inc = { totalDue: parsedTotalAmount };

    // Increase specific due amounts based on bill type
    if (isHajj) {
      vendorUpdate.$inc.hajDue = parsedTotalAmount;
    }
    if (isUmrah) {
      vendorUpdate.$inc.umrahDue = parsedTotalAmount;
    }

    // If payment was made, update totalPaid
    if (paymentStatus === 'paid' && paymentMethod) {
      vendorUpdate.$inc.totalPaid = parsedTotalAmount;
      vendorUpdate.$inc.totalDue = -parsedTotalAmount; // Net effect: 0 increase in due
      if (isHajj) {
        vendorUpdate.$inc.hajDue = -parsedTotalAmount;
      }
      if (isUmrah) {
        vendorUpdate.$inc.umrahDue = -parsedTotalAmount;
      }
    }

    // Update vendor
    if (ObjectId.isValid(vendorId)) {
      await vendors.updateOne(
        { _id: new ObjectId(vendorId), isActive: true },
        vendorUpdate
      );
    } else {
      await vendors.updateOne(
        { vendorId: vendor.vendorId, isActive: true },
        vendorUpdate
      );
    }

    // Get the created bill
    const createdBill = await vendorBills.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Vendor bill created successfully",
      bill: createdBill
    });

  } catch (error) {
    console.error("Error creating vendor bill:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while creating vendor bill"
    });
  }
});

// ✅ GET: Get all vendor bills with filters
app.get("/vendors/bills", async (req, res) => {
  try {
    const { vendorId, billType, startDate, endDate, paymentStatus, limit = 100 } = req.query;

    // Build query
    const query = { isActive: true };

    if (vendorId) {
      if (ObjectId.isValid(vendorId)) {
        query.vendorId = vendorId;
      } else {
        query.vendorId = vendorId;
      }
    }

    if (billType) {
      query.billType = billType;
    }

    if (paymentStatus) {
      query.paymentStatus = paymentStatus;
    }

    if (startDate || endDate) {
      query.billDate = {};
      if (startDate) {
        query.billDate.$gte = new Date(startDate);
      }
      if (endDate) {
        query.billDate.$lte = new Date(endDate);
      }
    }

    const bills = await vendorBills
      .find(query)
      .sort({ createdAt: -1 })
      .limit(parseInt(limit))
      .toArray();

    res.json({ success: true, bills });

  } catch (error) {
    console.error("Error fetching vendor bills:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor bills"
    });
  }
});

// ✅ GET: Get single vendor bill by ID
app.get("/vendors/bills/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid bill ID" });
    }

    const bill = await vendorBills.findOne({
      _id: new ObjectId(id),
      isActive: true
    });

    if (!bill) {
      return res.status(404).json({ error: true, message: "Bill not found" });
    }

    res.json({ success: true, bill });

  } catch (error) {
    console.error("Error fetching vendor bill:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor bill"
    });
  }
});

// ✅ GET: Get all bills for a specific vendor
app.get("/vendors/:id/bills", async (req, res) => {
  try {
    const { id } = req.params;
    const { limit = 100 } = req.query;

    // Find vendor first
    let vendor;
    if (ObjectId.isValid(id)) {
      vendor = await vendors.findOne({ _id: new ObjectId(id), isActive: true });
    } else {
      vendor = await vendors.findOne({ vendorId: id, isActive: true });
    }

    if (!vendor) {
      return res.status(404).json({ error: true, message: "Vendor not found" });
    }

    // Get all bills for this vendor
    const bills = await vendorBills
      .find({ vendorId: vendor.vendorId, isActive: true })
      .sort({ createdAt: -1 })
      .limit(parseInt(limit))
      .toArray();

    res.json({ success: true, bills });

  } catch (error) {
    console.error("Error fetching vendor bills:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor bills"
    });
  }
});

// ✅ PATCH: Update vendor bill
app.patch("/vendors/bills/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid bill ID" });
    }

    // Check if bill exists
    const existingBill = await vendorBills.findOne({
      _id: new ObjectId(id),
      isActive: true
    });

    if (!existingBill) {
      return res.status(404).json({ error: true, message: "Bill not found" });
    }

    // Prepare update data
    const allowedFields = ['paymentStatus', 'paymentMethod', 'notes', 'dueDate'];
    const filteredUpdateData = {};

    allowedFields.forEach(field => {
      if (updateData[field] !== undefined) {
        filteredUpdateData[field] = updateData[field];
      }
    });

    // Add update timestamp
    filteredUpdateData.updatedAt = new Date();

    // Update bill
    const result = await vendorBills.updateOne(
      { _id: new ObjectId(id), isActive: true },
      { $set: filteredUpdateData }
    );

    if (result.modifiedCount === 0) {
      return res.status(400).json({
        error: true,
        message: "No changes made or bill not found"
      });
    }

    // Get updated bill
    const updatedBill = await vendorBills.findOne({
      _id: new ObjectId(id),
      isActive: true
    });

    res.json({
      success: true,
      message: "Bill updated successfully",
      bill: updatedBill
    });

  } catch (error) {
    console.error("Error updating vendor bill:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while updating vendor bill"
    });
  }
});

// ✅ DELETE: Soft delete vendor bill
app.delete("/vendors/bills/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid bill ID" });
    }

    // Find the bill first to reverse vendor financials
    const bill = await vendorBills.findOne({
      _id: new ObjectId(id),
      isActive: true
    });

    if (!bill) {
      return res.status(404).json({ error: true, message: "Bill not found" });
    }

    // Reverse vendor financials
    const vendorUpdate = { $set: { updatedAt: new Date() } };
    
    const isHajj = bill.billType.toLowerCase().includes('hajj') || bill.billType.toLowerCase().includes('haj');
    const isUmrah = bill.billType.toLowerCase().includes('umrah');

    vendorUpdate.$inc = { totalDue: -bill.totalAmount };

    if (isHajj) {
      vendorUpdate.$inc.hajDue = -bill.totalAmount;
    }
    if (isUmrah) {
      vendorUpdate.$inc.umrahDue = -bill.totalAmount;
    }

    // If payment was made, also reverse totalPaid
    if (bill.paymentStatus === 'paid' && bill.paymentMethod) {
      vendorUpdate.$inc.totalPaid = -bill.totalAmount;
    }

    // Update vendor
    await vendors.updateOne(
      { vendorId: bill.vendorId, isActive: true },
      vendorUpdate
    );

    // Soft delete the bill
    const result = await vendorBills.updateOne(
      { _id: new ObjectId(id) },
      { $set: { isActive: false, deletedAt: new Date() } }
    );

    if (result.modifiedCount === 0) {
      return res.status(404).json({ error: true, message: "Bill not found" });
    }

    res.json({ success: true, message: "Vendor bill deleted successfully" });

  } catch (error) {
    console.error("Error deleting vendor bill:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting vendor bill"
    });
  }
});


// ==================== TRANSACTION ROUTES ====================

// ✅ POST: Create new transaction (IMPROVED VERSION)
app.post("/api/transactions", async (req, res) => {
  let session = null;

  try {
    const {
      transactionType,
      serviceCategory,
      partyType,
      partyId,
      invoiceId,
      paymentMethod,
      targetAccountId,
      accountManagerId,
      amount,
      branchId,
      createdBy,
      notes,
      reference,
      fromAccountId,
      toAccountId,
      // Frontend sends these nested objects
      debitAccount,
      creditAccount,
      paymentDetails,
      customerId,
      category,
      customerBankAccount,
      employeeReference
    } = req.body;

    // Extract values from nested objects if provided
    const finalAmount = amount || paymentDetails?.amount;
    const finalPartyId = partyId || customerId;
    const finalTargetAccountId = targetAccountId || creditAccount?.id || debitAccount?.id;
    const finalFromAccountId = fromAccountId || debitAccount?.id;
    const finalToAccountId = toAccountId || creditAccount?.id;
    const finalServiceCategory = serviceCategory || category;
    
    // Determine final party type defensively
    let finalPartyType = String(partyType || '').toLowerCase();

    // 1. Validation - আগে সব validate করুন
    if (!transactionType || !finalAmount || !finalPartyId) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields: transactionType, amount, and partyId"
      });
    }

    if (!['credit', 'debit', 'transfer'].includes(transactionType)) {
      return res.status(400).json({
        success: false,
        message: "Transaction type must be 'credit', 'debit', or 'transfer'"
      });
    }

    const numericAmount = parseFloat(finalAmount);
    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Amount must be a valid positive number"
      });
    }

    // 2. Validate party - আগে validate করুন
    let party = null;
    const searchPartyId = finalPartyId;
    const isValidObjectId = ObjectId.isValid(searchPartyId);
    // If looks like Hajj but client sent customer, auto-resolve to haji when match found
    try {
      const categoryText = String(finalServiceCategory || '').toLowerCase();
      const looksLikeHajj = categoryText.includes('haj');
      const looksLikeUmrah = categoryText.includes('umrah');
      if (finalPartyType === 'customer' && looksLikeHajj && searchPartyId) {
        const hajiCond = isValidObjectId
          ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: { $ne: false } }
          : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }], isActive: { $ne: false } };
        const maybeHaji = await haji.findOne(hajiCond);
        if (maybeHaji && maybeHaji._id) {
          finalPartyType = 'haji';
        }
      } else if (finalPartyType === 'customer' && looksLikeUmrah && searchPartyId) {
        const umrahCond = isValidObjectId
          ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: { $ne: false } }
          : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }], isActive: { $ne: false } };
        const maybeUmrah = await umrah.findOne(umrahCond);
        if (maybeUmrah && maybeUmrah._id) {
          finalPartyType = 'umrah';
        }
      }
    } catch (_) {}
    const searchCondition = isValidObjectId
      ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: true }
      : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }], isActive: true };

    if (finalPartyType === 'customer') {
      party = await customers.findOne(searchCondition);
    } else if (finalPartyType === 'agent') {
      const agentCondition = isValidObjectId
        ? { $or: [{ agentId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: true }
        : { $or: [{ agentId: searchPartyId }, { _id: searchPartyId }], isActive: true };
      party = await agents.findOne(agentCondition);
    } else if (finalPartyType === 'vendor') {
      const vendorCondition = isValidObjectId
        ? { $or: [{ vendorId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: true }
        : { $or: [{ vendorId: searchPartyId }, { _id: searchPartyId }], isActive: true };
      party = await vendors.findOne(vendorCondition);
    } else if (finalPartyType === 'haji') {
      const hajiCondition = isValidObjectId
        ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: true }
        : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }], isActive: true };
      party = await haji.findOne(hajiCondition);
    } else if (finalPartyType === 'umrah') {
      // Don't filter by isActive to allow finding inactive/deleted profiles
      // This ensures transactions can update paidAmount even for deleted profiles
      const umrahCondition = isValidObjectId
        ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }] }
        : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }] };
      party = await umrah.findOne(umrahCondition);
    }

    // Allow transactions even if party is not found in database
    // Party information will be stored as provided
    if (!party && partyType && partyType !== 'other') {
      console.warn(`Party not found in database: ${finalPartyType} with ID ${searchPartyId}`);
      // Don't return error, allow transaction to proceed
    }

    // 3. Validate branch - আগে validate করুন (fallback সহ)
    let branch;
    if (branchId) {
      branch = await branches.findOne({ branchId, isActive: true });
    } else {
      branch = await branches.findOne({ isActive: true });
    }

    // Fallback: যদি কোনো active branch না পাওয়া যায়, auto-create/reactivate default branch
    if (!branch) {
      const defaultBranchId = 'main';
      const defaultDoc = {
        branchId: defaultBranchId,
        branchName: 'Main Branch',
        branchCode: 'MN',
        createdAt: new Date(),
        updatedAt: new Date()
      };
      // Step 1: Upsert basic doc without touching isActive in the same update
      await branches.updateOne(
        { branchId: defaultBranchId },
        { $setOnInsert: defaultDoc },
        { upsert: true }
      );
      // Step 2: Ensure isActive true in a separate update to avoid path conflicts
      await branches.updateOne(
        { branchId: defaultBranchId },
        { $set: { isActive: true, updatedAt: new Date() } }
      );
      branch = await branches.findOne({ branchId: defaultBranchId, isActive: true });
    }

    // 4. Validate accounts BEFORE updating balances - আগে validate করুন
    let account = null;
    let fromAccount = null;
    let toAccount = null;

    if (transactionType === "credit" || transactionType === "debit") {
      if (!finalTargetAccountId) {
        return res.status(400).json({
          success: false,
          message: "targetAccountId is required for credit/debit transactions"
        });
      }
      account = await bankAccounts.findOne({ _id: new ObjectId(finalTargetAccountId) });
      if (!account) {
        return res.status(404).json({ success: false, message: "Target account not found" });
      }

      if (transactionType === "debit" && (account.currentBalance || 0) < numericAmount) {
        return res.status(400).json({
          success: false,
          message: "Insufficient balance"
        });
      }
    } else if (transactionType === "transfer") {
      if (!finalFromAccountId || !finalToAccountId) {
        return res.status(400).json({
          success: false,
          message: "fromAccountId and toAccountId are required for transfer transactions"
        });
      }

      fromAccount = await bankAccounts.findOne({ _id: new ObjectId(finalFromAccountId) });
      toAccount = await bankAccounts.findOne({ _id: new ObjectId(finalToAccountId) });

      if (!fromAccount || !toAccount) {
        return res.status(404).json({
          success: false,
          message: "One or both accounts not found"
        });
      }

      if ((fromAccount.currentBalance || 0) < numericAmount) {
        return res.status(400).json({
          success: false,
          message: "Insufficient balance in source account"
        });
      }
    }

    // 5. Start MongoDB session for atomic operations
    session = db.client.startSession();
    session.startTransaction();

    let transactionResult;
    let updatedAgent = null;
    let updatedCustomer = null;
    let updatedVendor = null;

    try {
      // 6. Update balances WITHIN transaction
      if (transactionType === "credit") {
        const newBalance = (account.currentBalance || 0) + numericAmount;
        await bankAccounts.updateOne(
          { _id: new ObjectId(finalTargetAccountId) },
          {
            $set: { currentBalance: newBalance, updatedAt: new Date() },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'deposit',
                note: notes || `Transaction credit`,
                at: new Date()
              }
            }
          },
          { session }
        );

      } else if (transactionType === "debit") {
        const newBalance = (account.currentBalance || 0) - numericAmount;
        await bankAccounts.updateOne(
          { _id: new ObjectId(finalTargetAccountId) },
          {
            $set: { currentBalance: newBalance, updatedAt: new Date() },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'withdrawal',
                note: notes || `Transaction debit`,
                at: new Date()
              }
            }
          },
          { session }
        );

      } else if (transactionType === "transfer") {
        const fromNewBalance = (fromAccount.currentBalance || 0) - numericAmount;
        const toNewBalance = (toAccount.currentBalance || 0) + numericAmount;

        await bankAccounts.updateOne(
          { _id: new ObjectId(finalFromAccountId) },
          {
            $set: { currentBalance: fromNewBalance, updatedAt: new Date() },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'withdrawal',
                note: `Transfer to ${toAccount.bankName || ''} - ${toAccount.accountNumber || ''}`.trim(),
                at: new Date()
              }
            }
          },
          { session }
        );

        await bankAccounts.updateOne(
          { _id: new ObjectId(finalToAccountId) },
          {
            $set: { currentBalance: toNewBalance, updatedAt: new Date() },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'deposit',
                note: `Transfer from ${fromAccount.bankName || ''} - ${fromAccount.accountNumber || ''}`.trim(),
                at: new Date()
              }
            }
          },
          { session }
        );
      }

      // 7. Generate transaction ID
      const transactionId = await generateTransactionId(db, branch.branchCode);

      // 8. Create transaction record
      const transactionData = {
        transactionId,
        transactionType,
        serviceCategory: finalServiceCategory,
        partyType: finalPartyType,
        partyId: finalPartyId,
        partyName: party?.name || party?.customerName || party?.agentName || party?.tradeName || party?.vendorName || 'Unknown',
        partyPhone: party?.phone || party?.customerPhone || party?.contactNo || party?.mobile || null,
        partyEmail: party?.email || party?.customerEmail || null,
        invoiceId,
        paymentMethod,
        targetAccountId: transactionType === 'transfer' ? finalToAccountId : finalTargetAccountId,
        fromAccountId: transactionType === 'transfer' ? finalFromAccountId : null,
        accountManagerId,
        // Include nested objects for compatibility
        debitAccount: debitAccount || (transactionType === 'debit' ? { id: finalTargetAccountId } : null),
        creditAccount: creditAccount || (transactionType === 'credit' ? { id: finalTargetAccountId } : null),
        paymentDetails: paymentDetails || { amount: numericAmount },
        customerBankAccount: customerBankAccount || null,
        amount: numericAmount,
        branchId: branch.branchId,
        branchName: branch.branchName,
        branchCode: branch.branchCode,
        createdBy: createdBy || 'SYSTEM',
        notes: notes || '',
        reference: reference || paymentDetails?.reference || transactionId,
        employeeReference: employeeReference || null,
        status: 'completed',
        date: new Date(),
        createdAt: new Date(),
        updatedAt: new Date(),
        isActive: true
      };

      // 8.1 If party is an agent, update agent due amounts atomically
      if (finalPartyType === 'agent' && party && party._id) {
        const categoryText = String(finalServiceCategory || '').toLowerCase();
        const isHajjCategory = categoryText.includes('haj');
        const isUmrahCategory = categoryText.includes('umrah');
        const dueDelta = transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0);

        const agentUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
        if (isHajjCategory) {
          agentUpdate.$inc.hajDue = (agentUpdate.$inc.hajDue || 0) + dueDelta;
        }
        if (isUmrahCategory) {
          agentUpdate.$inc.umrahDue = (agentUpdate.$inc.umrahDue || 0) + dueDelta;
        }
        // Credit korle agent er totalDeposit barbe
        if (transactionType === 'credit') {
          agentUpdate.$inc.totalDeposit = (agentUpdate.$inc.totalDeposit || 0) + numericAmount;
        }
        await agents.updateOne({ _id: party._id }, agentUpdate, { session });
        updatedAgent = await agents.findOne({ _id: party._id }, { session });
      }

      // 8.2 If party is a vendor, update vendor due amounts atomically (Hajj/Umrah wise)
      if (finalPartyType === 'vendor' && party && party._id) {
        const categoryText = String(finalServiceCategory || '').toLowerCase();
        const isHajjCategory = categoryText.includes('haj');
        const isUmrahCategory = categoryText.includes('umrah');
        // Vendor specific logic: debit => vendor ke taka deya (due kombe), credit => vendor theke taka neya (due barbe)
        const vendorDueDelta = transactionType === 'debit' ? -numericAmount : (transactionType === 'credit' ? numericAmount : 0);

        const vendorUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: vendorDueDelta } };
        if (isHajjCategory) {
          vendorUpdate.$inc.hajDue = (vendorUpdate.$inc.hajDue || 0) + vendorDueDelta;
        }
        if (isUmrahCategory) {
          vendorUpdate.$inc.umrahDue = (vendorUpdate.$inc.umrahDue || 0) + vendorDueDelta;
        }
        // Debit সাধারণত ভেন্ডরকে পেমেন্ট—track totalPaid
        if (transactionType === 'debit') {
          vendorUpdate.$inc.totalPaid = (vendorUpdate.$inc.totalPaid || 0) + numericAmount;
        }

        await vendors.updateOne({ _id: party._id }, vendorUpdate, { session });
        updatedVendor = await vendors.findOne({ _id: party._id }, { session });
      }

      // 8.3 If party is a customer, update customer due amounts atomically (Hajj/Umrah wise)
      if (finalPartyType === 'customer' && party && party._id) {
        const categoryText = String(finalServiceCategory || '').toLowerCase();
        const isHajjCategory = categoryText.includes('haj');
        const isUmrahCategory = categoryText.includes('umrah');
        const dueDelta = transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0);

        const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
        if (isHajjCategory) {
          customerUpdate.$inc.hajjDue = (customerUpdate.$inc.hajjDue || 0) + dueDelta;
        }
        if (isUmrahCategory) {
          customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) + dueDelta;
        }
        // New: On credit, also increment paidAmount
        if (transactionType === 'credit') {
          customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) + numericAmount;
        }
        await customers.updateOne({ _id: party._id }, customerUpdate, { session });
        const after = await customers.findOne({ _id: party._id }, { session });
        // Clamp due fields to 0+
        const setClamp = {};
        if ((after.totalDue || 0) < 0) setClamp['totalDue'] = 0;
        if ((after.paidAmount || 0) < 0) setClamp['paidAmount'] = 0;
        if ((after.hajjDue !== undefined) && after.hajjDue < 0) setClamp['hajjDue'] = 0;
        if ((after.umrahDue !== undefined) && after.umrahDue < 0) setClamp['umrahDue'] = 0;
        if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
          setClamp['paidAmount'] = after.totalAmount;
        }
        if (Object.keys(setClamp).length) {
          setClamp.updatedAt = new Date();
          await customers.updateOne({ _id: party._id }, { $set: setClamp }, { session });
        }
        updatedCustomer = await customers.findOne({ _id: party._id }, { session });

        // Additionally, if this customer also exists in the Haji collection by id/customerId, update paidAmount there on credit
        if (transactionType === 'credit') {
          const hajiCond = ObjectId.isValid(finalPartyId)
            ? { $or: [{ customerId: finalPartyId }, { _id: new ObjectId(finalPartyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: finalPartyId }, { _id: finalPartyId }], isActive: { $ne: false } };
          const hajiDoc = await haji.findOne(hajiCond, { session });
          if (hajiDoc && hajiDoc._id) {
            await haji.updateOne(
              { _id: hajiDoc._id },
              { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
              { session }
            );
            const afterH = await haji.findOne({ _id: hajiDoc._id }, { session });
            const clampH = {};
            if ((afterH.paidAmount || 0) < 0) clampH.paidAmount = 0;
            if (typeof afterH.totalAmount === 'number' && typeof afterH.paidAmount === 'number' && afterH.paidAmount > afterH.totalAmount) {
              clampH.paidAmount = afterH.totalAmount;
            }
            if (Object.keys(clampH).length) {
              clampH.updatedAt = new Date();
              await haji.updateOne({ _id: hajiDoc._id }, { $set: clampH }, { session });
            }
          }
          
          // Additionally, if this customer also exists in the Umrah collection by id/customerId, update paidAmount there on credit
          // Don't filter by isActive to allow updating deleted/inactive profiles
          const umrahCond = ObjectId.isValid(finalPartyId)
            ? { $or: [{ customerId: finalPartyId }, { _id: new ObjectId(finalPartyId) }] }
            : { $or: [{ customerId: finalPartyId }, { _id: finalPartyId }] };
          const umrahDoc = await umrah.findOne(umrahCond, { session });
          if (umrahDoc && umrahDoc._id) {
            await umrah.updateOne(
              { _id: umrahDoc._id },
              { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
              { session }
            );
            const afterU = await umrah.findOne({ _id: umrahDoc._id }, { session });
            const clampU = {};
            if ((afterU.paidAmount || 0) < 0) clampU.paidAmount = 0;
            if (typeof afterU.totalAmount === 'number' && typeof afterU.paidAmount === 'number' && afterU.paidAmount > afterU.totalAmount) {
              clampU.paidAmount = afterU.totalAmount;
            }
            if (Object.keys(clampU).length) {
              clampU.updatedAt = new Date();
              await umrah.updateOne({ _id: umrahDoc._id }, { $set: clampU }, { session });
            }
          }
        }
      }
      
      // 8.4 If party is a haji, update haji and sync linked customer profile amounts
      if (finalPartyType === 'haji' && party && party._id) {
        const categoryText = String(finalServiceCategory || '').toLowerCase();
        const isHajjCategory = categoryText.includes('haj');
        const isUmrahCategory = categoryText.includes('umrah');

        // Update Haji paidAmount on credit
        if (transactionType === 'credit') {
          await haji.updateOne(
            { _id: party._id },
            { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
            { session }
          );
          const afterHaji = await haji.findOne({ _id: party._id }, { session });
          const setClampHaji = {};
          if ((afterHaji.paidAmount || 0) < 0) setClampHaji.paidAmount = 0;
          if (typeof afterHaji.totalAmount === 'number' && typeof afterHaji.paidAmount === 'number' && afterHaji.paidAmount > afterHaji.totalAmount) {
            setClampHaji.paidAmount = afterHaji.totalAmount;
          }
          if (Object.keys(setClampHaji).length) {
            setClampHaji.updatedAt = new Date();
            await haji.updateOne({ _id: party._id }, { $set: setClampHaji }, { session });
          }
        }

        // Sync to linked customer (if exists via customerId)
        try {
          const linkedCustomerId = party.customerId || party.customer_id;
          if (linkedCustomerId) {
            const customerCond = ObjectId.isValid(linkedCustomerId)
              ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: { $ne: false } }
              : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: { $ne: false } };
            const custDoc = await customers.findOne(customerCond, { session });
            if (custDoc && custDoc._id) {
              const dueDelta = transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0);
              const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
              if (isHajjCategory) customerUpdate.$inc.hajjDue = (customerUpdate.$inc.hajjDue || 0) + dueDelta;
              if (isUmrahCategory) customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) + dueDelta;
              if (transactionType === 'credit') customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) + numericAmount;

              await customers.updateOne({ _id: custDoc._id }, customerUpdate, { session });

              // Clamp negatives and overpayments
              const afterCust = await customers.findOne({ _id: custDoc._id }, { session });
              const clampCust = {};
              if ((afterCust.totalDue || 0) < 0) clampCust.totalDue = 0;
              if ((afterCust.paidAmount || 0) < 0) clampCust.paidAmount = 0;
              if ((afterCust.hajjDue !== undefined) && afterCust.hajjDue < 0) clampCust.hajjDue = 0;
              if ((afterCust.umrahDue !== undefined) && afterCust.umrahDue < 0) clampCust.umrahDue = 0;
              if (typeof afterCust.totalAmount === 'number' && typeof afterCust.paidAmount === 'number' && afterCust.paidAmount > afterCust.totalAmount) {
                clampCust.paidAmount = afterCust.totalAmount;
              }
              if (Object.keys(clampCust).length) {
                clampCust.updatedAt = new Date();
                await customers.updateOne({ _id: custDoc._id }, { $set: clampCust }, { session });
              }
            }
          }
        } catch (syncErr) {
          console.warn('Customer sync from haji transaction failed:', syncErr?.message);
        }
      }

      // 8.5 If party is an umrah, update umrah and sync linked customer profile amounts
      if (finalPartyType === 'umrah' && party && party._id) {
        const categoryText = String(finalServiceCategory || '').toLowerCase();
        const isUmrahCategory = categoryText.includes('umrah');

        // Update Umrah paidAmount on credit
        if (transactionType === 'credit') {
          await umrah.updateOne(
            { _id: party._id },
            { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } },
            { session }
          );
          const afterUmrah = await umrah.findOne({ _id: party._id }, { session });
          const setClampUmrah = {};
          if ((afterUmrah.paidAmount || 0) < 0) setClampUmrah.paidAmount = 0;
          if (typeof afterUmrah.totalAmount === 'number' && typeof afterUmrah.paidAmount === 'number' && afterUmrah.paidAmount > afterUmrah.totalAmount) {
            setClampUmrah.paidAmount = afterUmrah.totalAmount;
          }
          if (Object.keys(setClampUmrah).length) {
            setClampUmrah.updatedAt = new Date();
            await umrah.updateOne({ _id: party._id }, { $set: setClampUmrah }, { session });
          }
        }

        // Sync to linked customer (if exists via customerId)
        try {
          const linkedCustomerId = party.customerId || party.customer_id;
          if (linkedCustomerId) {
            const customerCond = ObjectId.isValid(linkedCustomerId)
              ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: { $ne: false } }
              : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: { $ne: false } };
            const custDoc = await customers.findOne(customerCond, { session });
            if (custDoc && custDoc._id) {
              const dueDelta = transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0);
              const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
              if (isUmrahCategory) customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) + dueDelta;
              if (transactionType === 'credit') customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) + numericAmount;

              await customers.updateOne({ _id: custDoc._id }, customerUpdate, { session });

              // Clamp negatives and overpayments
              const afterCust = await customers.findOne({ _id: custDoc._id }, { session });
              const clampCust = {};
              if ((afterCust.totalDue || 0) < 0) clampCust.totalDue = 0;
              if ((afterCust.paidAmount || 0) < 0) clampCust.paidAmount = 0;
              if ((afterCust.umrahDue !== undefined) && afterCust.umrahDue < 0) clampCust.umrahDue = 0;
              if (typeof afterCust.totalAmount === 'number' && typeof afterCust.paidAmount === 'number' && afterCust.paidAmount > afterCust.totalAmount) {
                clampCust.paidAmount = afterCust.totalAmount;
              }
              if (Object.keys(clampCust).length) {
                clampCust.updatedAt = new Date();
                await customers.updateOne({ _id: custDoc._id }, { $set: clampCust }, { session });
              }
            }
          }
        } catch (syncErr) {
          console.warn('Customer sync from umrah transaction failed:', syncErr?.message);
        }
      }

      transactionResult = await transactions.insertOne(transactionData, { session });

      // 9. Commit transaction
      await session.commitTransaction();

      res.json({
        success: true,
        transaction: { ...transactionData, _id: transactionResult.insertedId },
        agent: updatedAgent || null,
        customer: updatedCustomer || null,
        vendor: updatedVendor || null
      });

    } catch (transactionError) {
      // Rollback on error
      await session.abortTransaction();
      throw transactionError;
    }

  } catch (err) {
    // Clean up session on error
    if (session && session.inTransaction()) {
      await session.abortTransaction();
    }

    console.error('Transaction creation error:', err);
    res.status(500).json({
      success: false,
      message: err.message
    });
  } finally {
    // End session
    if (session) {
      session.endSession();
    }
  }
});

// ✅ GET: List transactions with filters and pagination
app.get("/api/transactions", async (req, res) => {
  try {
    const {
      partyType,
      partyId,
      transactionType,
      serviceCategory,
      branchId,
      accountId,
      fromDate,
      toDate,
      page = 1,
      limit = 20,
      q
    } = req.query || {};

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    const filter = { isActive: { $ne: false } };

    if (partyType) filter.partyType = String(partyType);
    if (partyId) filter.partyId = String(partyId);
    if (transactionType) filter.transactionType = String(transactionType);
    if (serviceCategory) filter.serviceCategory = String(serviceCategory);
    if (branchId) filter.branchId = String(branchId);
    if (accountId) filter.$or = [
      { targetAccountId: String(accountId) },
      { fromAccountId: String(accountId) }
    ];

    if (fromDate || toDate) {
      filter.date = {};
      if (fromDate) filter.date.$gte = new Date(fromDate);
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
        }
        filter.date.$lte = end;
      }
    }

    if (q) {
      const text = String(q).trim();
      filter.$or = [
        ...(filter.$or || []),
        { transactionId: { $regex: text, $options: 'i' } },
        { partyName: { $regex: text, $options: 'i' } },
        { notes: { $regex: text, $options: 'i' } },
        { reference: { $regex: text, $options: 'i' } }
      ];
    }

    const cursor = transactions
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum);

    const [items, total] = await Promise.all([
      cursor.toArray(),
      transactions.countDocuments(filter)
    ]);

    res.json({
      success: true,
      data: items,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('List transactions error:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch transactions', error: error.message });
  }
});

// ==================== ORDER ROUTES ====================

// ✅ POST: Create new order
app.post("/orders", async (req, res) => {
  try {
    const {
      vendorId,
      orderType,
      amount,
      notes,
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!vendorId || !orderType || !amount) {
      return res.status(400).json({
        error: true,
        message: "Vendor ID, order type, and amount are required"
      });
    }

    // Validate amount
    const parsedAmount = parseFloat(amount);
    if (!parsedAmount || parsedAmount <= 0) {
      return res.status(400).json({
        error: true,
        message: "Amount must be greater than 0"
      });
    }

    // Check if vendor exists - support both MongoDB ObjectId and string vendorId (e.g., VN-...)
    let vendor;
    if (ObjectId.isValid(vendorId)) {
      vendor = await vendors.findOne({ _id: new ObjectId(vendorId), isActive: true });
    } else {
      const normalized = String(vendorId).trim().toUpperCase();
      vendor = await vendors.findOne({ vendorId: normalized, isActive: true });
    }

    if (!vendor) {
      return res.status(404).json({
        error: true,
        message: "Vendor not found"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId: branchId || 'main', isActive: true });
    if (!branch) {
      return res.status(400).json({
        error: true,
        message: "Invalid branch ID"
      });
    }

    // Generate unique order ID
    const orderId = await generateOrderId(db, branch.branchCode);

    // Create order object
    const newOrder = {
      orderId,
      vendorId: vendor._id,
      vendorName: vendor.tradeName,
      vendorLocation: vendor.tradeLocation,
      vendorContact: vendor.contactNo,
      vendorOwner: vendor.ownerName,
      orderType: orderType.trim(),
      amount: parsedAmount,
      notes: notes || null,
      status: 'pending', // pending, confirmed, completed, cancelled
      createdBy: createdBy || null,
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true
    };

    const result = await orders.insertOne(newOrder);

    res.status(201).json({
      success: true,
      message: "Order created successfully",
      order: {
        _id: result.insertedId,
        orderId: newOrder.orderId,
        vendorId: newOrder.vendorId,
        vendorName: newOrder.vendorName,
        vendorLocation: newOrder.vendorLocation,
        vendorContact: newOrder.vendorContact,
        vendorOwner: newOrder.vendorOwner,
        orderType: newOrder.orderType,
        amount: newOrder.amount,
        notes: newOrder.notes,
        status: newOrder.status,
        branchId: newOrder.branchId,
        branchName: newOrder.branchName,
        createdAt: newOrder.createdAt
      }
    });

  } catch (error) {
    console.error('Create order error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while creating order"
    });
  }
});

// ✅ GET: Get all orders with filters
app.get("/orders", async (req, res) => {
  try {
    const {
      vendorId,
      orderType,
      status,
      branchId,
      dateFrom,
      dateTo,
      search,
      page = 1,
      limit = 20
    } = req.query;

    let filter = { isActive: true };

    // Apply filters
    if (vendorId) filter.vendorId = new ObjectId(vendorId);
    if (orderType) filter.orderType = { $regex: orderType, $options: 'i' };
    if (status) filter.status = status;
    if (branchId) filter.branchId = branchId;

    // Date range filter
    if (dateFrom || dateTo) {
      filter.createdAt = {};
      if (dateFrom) filter.createdAt.$gte = new Date(dateFrom);
      if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        filter.createdAt.$lte = endDate;
      }
    }

    // Search filter
    if (search) {
      filter.$or = [
        { orderId: { $regex: search, $options: 'i' } },
        { vendorName: { $regex: search, $options: 'i' } },
        { vendorContact: { $regex: search, $options: 'i' } },
        { vendorOwner: { $regex: search, $options: 'i' } },
        { orderType: { $regex: search, $options: 'i' } },
        { notes: { $regex: search, $options: 'i' } }
      ];
    }

    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Get total count
    const totalCount = await orders.countDocuments(filter);

    // Get orders with pagination
    const allOrders = await orders.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .toArray();

    res.json({
      success: true,
      count: allOrders.length,
      totalCount,
      currentPage: parseInt(page),
      totalPages: Math.ceil(totalCount / parseInt(limit)),
      orders: allOrders
    });
  } catch (error) {
    console.error('Get orders error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching orders"
    });
  }
});

// ✅ GET: Get order by ID
app.get("/orders/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;

    const order = await orders.findOne({
      orderId: orderId,
      isActive: true
    });

    if (!order) {
      return res.status(404).json({
        error: true,
        message: "Order not found"
      });
    }

    res.json({
      success: true,
      order: order
    });
  } catch (error) {
    console.error('Get order error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching order"
    });
  }
});

// ✅ PATCH: Update order
app.patch("/orders/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;
    const updateData = req.body;

    // Remove fields that shouldn't be updated
    delete updateData.orderId;
    delete updateData.createdAt;
    updateData.updatedAt = new Date();

    // Validate status if being updated
    if (updateData.status) {
      const validStatuses = ['pending', 'confirmed', 'completed', 'cancelled'];
      if (!validStatuses.includes(updateData.status)) {
        return res.status(400).json({
          error: true,
          message: "Invalid status. Must be one of: pending, confirmed, completed, cancelled"
        });
      }
    }

    // Validate amount if being updated
    if (updateData.amount) {
      const parsedAmount = parseFloat(updateData.amount);
      if (!parsedAmount || parsedAmount <= 0) {
        return res.status(400).json({
          error: true,
          message: "Amount must be greater than 0"
        });
      }
      updateData.amount = parsedAmount;
    }

    const result = await orders.updateOne(
      { orderId: orderId, isActive: true },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        error: true,
        message: "Order not found"
      });
    }

    res.json({
      success: true,
      message: "Order updated successfully",
      modifiedCount: result.modifiedCount
    });
  } catch (error) {
    console.error('Update order error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while updating order"
    });
  }
});

// ✅ DELETE: Delete order (soft delete)
app.delete("/orders/:orderId", async (req, res) => {
  try {
    const { orderId } = req.params;

    const result = await orders.updateOne(
      { orderId: orderId, isActive: true },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        error: true,
        message: "Order not found"
      });
    }

    res.json({
      success: true,
      message: "Order deleted successfully"
    });
  } catch (error) {
    console.error('Delete order error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting order"
    });
  }
});

// ==================== LOAN ROUTES ====================

// ✅ POST: Create new loan giving
app.post("/loans/giving", async (req, res) => {
  try {
    const {
      // Personal Profile Information
      fullName,
      fatherName,
      motherName,
      dateOfBirth,
      gender,
      maritalStatus,
      nidNumber,
      nidFrontImage,
      nidBackImage,
      profilePhoto,
      // Address Information
      presentAddress,
      permanentAddress,
      district,
      upazila,
      postCode,
      // Business Information
      businessName,
      businessType,
      businessAddress,
      businessRegistration,
      businessExperience,
      // Loan Details
      loanType,
      amount,
      source,
      purpose,
      interestRate,
      duration,
      givenDate,
      // Contact Information
      contactPerson,
      contactPhone,
      contactEmail,
      emergencyContact,
      emergencyPhone,
      // Additional Information
      notes,
      status = 'Active',
      remainingAmount,
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!fullName || !fatherName || !motherName || !dateOfBirth || !gender || 
        !nidNumber || !presentAddress || !permanentAddress || !district || !upazila ||
        !businessName || !businessType || !loanType || !amount || !source || 
        !purpose || !interestRate || !duration || !contactPerson || !contactPhone ||
        !emergencyContact || !emergencyPhone) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields"
      });
    }

    // Validate numeric fields
    const numericAmount = parseFloat(amount);
    const numericInterestRate = parseFloat(interestRate);
    const numericDuration = parseInt(duration);

    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Amount must be greater than 0"
      });
    }

    if (isNaN(numericInterestRate) || numericInterestRate <= 0 || numericInterestRate > 100) {
      return res.status(400).json({
        success: false,
        message: "Interest rate must be between 0 and 100"
      });
    }

    if (isNaN(numericDuration) || numericDuration <= 0) {
      return res.status(400).json({
        success: false,
        message: "Duration must be greater than 0"
      });
    }

    // Validate phone number format
    const phoneRegex = /^01[3-9]\d{8}$/;
    if (!phoneRegex.test(contactPhone)) {
      return res.status(400).json({
        success: false,
        message: "Invalid phone number format"
      });
    }

    if (emergencyPhone && !phoneRegex.test(emergencyPhone)) {
      return res.status(400).json({
        success: false,
        message: "Invalid emergency phone number format"
      });
    }

    // Validate email if provided
    if (contactEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contactEmail)) {
      return res.status(400).json({
        success: false,
        message: "Invalid email format"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId: branchId || 'main', isActive: true });
    if (!branch) {
      return res.status(400).json({
        success: false,
        message: "Invalid branch ID"
      });
    }

    // Generate unique loan ID
    const loanId = await generateLoanId(db, branch.branchCode, 'giving');

    // Calculate remaining amount if not provided
    const finalRemainingAmount = remainingAmount !== undefined ? parseFloat(remainingAmount) : numericAmount;

    // Create loan document
    const newLoan = {
      loanId,
      loanDirection: 'giving', // This is a loan giving
      // Personal Profile Information
      fullName: fullName.trim(),
      fatherName: fatherName.trim(),
      motherName: motherName.trim(),
      dateOfBirth: new Date(dateOfBirth),
      gender,
      maritalStatus: maritalStatus || null,
      nidNumber: nidNumber.trim(),
      nidFrontImage: nidFrontImage || null,
      nidBackImage: nidBackImage || null,
      profilePhoto: profilePhoto || null,
      // Address Information
      presentAddress: presentAddress.trim(),
      permanentAddress: permanentAddress.trim(),
      district: district.trim(),
      upazila: upazila.trim(),
      postCode: postCode || null,
      // Business Information
      businessName: businessName.trim(),
      businessType: businessType.trim(),
      businessAddress: businessAddress || null,
      businessRegistration: businessRegistration || null,
      businessExperience: businessExperience || null,
      // Loan Details
      loanType: loanType.trim(),
      amount: numericAmount,
      source: source.trim(),
      purpose: purpose.trim(),
      interestRate: numericInterestRate,
      duration: numericDuration,
      givenDate: new Date(givenDate || new Date()),
      // Contact Information
      contactPerson: contactPerson.trim(),
      contactPhone: contactPhone.trim(),
      contactEmail: contactEmail || null,
      emergencyContact: emergencyContact.trim(),
      emergencyPhone: emergencyPhone.trim(),
      // Additional Information
      notes: notes || null,
      // Status and tracking
      status: status,
      remainingAmount: finalRemainingAmount,
      // Metadata
      createdBy: createdBy || 'unknown_user',
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true
    };

    // Insert loan into database
    const result = await loans.insertOne(newLoan);

    if (!result.insertedId) {
      return res.status(500).json({
        success: false,
        message: "Failed to create loan"
      });
    }

    // Get the created loan
    const createdLoan = await loans.findOne({ _id: result.insertedId });

    // Create transaction for loan giving (DEBIT - money going out)
    // Note: Frontend should provide targetAccountId in request body for transaction
    let transactionCreated = null;
    const targetAccountId = req.body.targetAccountId; // Account from which money will be debited
    
    if (targetAccountId) {
      try {
        // Generate transaction ID
        const transactionId = await generateTransactionId(db, branch.branchCode);

        // Create transaction record for loan giving
        const transactionData = {
          transactionId,
          transactionType: 'debit', // Money going out (আমাদের account থেকে money বের হচ্ছে)
          serviceCategory: 'Loan Giving',
          partyType: 'loan', // New party type for loans
          partyId: result.insertedId.toString(),
          partyName: createdLoan.fullName,
          partyPhone: createdLoan.contactPhone,
          partyEmail: createdLoan.contactEmail || null,
          invoiceId: createdLoan.loanId, // Use loanId as reference
          paymentMethod: 'bank-transfer',
          targetAccountId: targetAccountId,
          accountManagerId: null,
          debitAccount: { id: targetAccountId },
          creditAccount: null,
          paymentDetails: {
            amount: numericAmount,
            reference: createdLoan.loanId,
            loanId: createdLoan.loanId,
            loanType: createdLoan.loanType
          },
          amount: numericAmount,
          branchId: branch.branchId,
          branchName: branch.branchName,
          branchCode: branch.branchCode,
          createdBy: createdBy || 'unknown_user',
          notes: `Loan Given: ${createdLoan.loanType} - ${notes || 'No additional notes'}`,
          reference: createdLoan.loanId,
          employeeReference: null,
          status: 'completed',
          date: new Date(givenDate || new Date()),
          createdAt: new Date(),
          updatedAt: new Date(),
          isActive: true,
          loanId: createdLoan.loanId, // Link to loan document
          loanDirection: 'giving'
        };

        // Insert transaction
        await transactions.insertOne(transactionData);
        transactionCreated = transactionId;

        // Update bank account balance (debit)
        const account = await bankAccounts.findOne({ _id: new ObjectId(targetAccountId) });
        if (account) {
          const newBalance = (account.currentBalance || 0) - numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(targetAccountId) },
            {
              $set: { currentBalance: newBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: numericAmount,
                  type: 'withdrawal',
                  note: `Loan Given: ${createdLoan.loanId} - ${createdLoan.fullName}`,
                  at: new Date(),
                  transactionId,
                  loanId: createdLoan.loanId
                }
              }
            }
          );
        }
      } catch (transactionError) {
        console.error('Transaction creation error for loan:', transactionError);
        // Continue even if transaction creation fails - loan is already created
      }
    }

    res.status(201).json({
      success: true,
      message: "Loan has been successfully given",
      loan: {
        loanId: createdLoan.loanId,
        fullName: createdLoan.fullName,
        loanDirection: createdLoan.loanDirection,
        loanType: createdLoan.loanType,
        amount: createdLoan.amount,
        status: createdLoan.status,
        remainingAmount: createdLoan.remainingAmount,
        branchId: createdLoan.branchId,
        createdAt: createdLoan.createdAt
      },
      transactionId: transactionCreated || null
    });

  } catch (error) {
    console.error('Loan giving error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while giving loan",
      error: error.message
    });
  }
});

// ✅ POST: Create new loan receiving (loan application)
app.post("/loans/receiving", async (req, res) => {
  try {
    const {
      // Personal Profile Information
      fullName,
      fatherName,
      motherName,
      dateOfBirth,
      gender,
      maritalStatus,
      nidNumber,
      nidFrontImage,
      nidBackImage,
      profilePhoto,
      // Address Information
      presentAddress,
      permanentAddress,
      district,
      upazila,
      postCode,
      // Business Information
      businessName,
      businessType,
      businessAddress,
      businessRegistration,
      businessExperience,
      // Loan Details
      loanType,
      amount,
      source,
      purpose,
      interestRate,
      duration,
      appliedDate,
      // Contact Information
      contactPerson,
      contactPhone,
      contactEmail,
      emergencyContact,
      emergencyPhone,
      // Additional Information
      notes,
      status = 'Pending',
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!fullName || !fatherName || !dateOfBirth || !gender || 
        !nidNumber || !presentAddress || !district || !upazila ||
        !loanType || !amount || !source || !purpose || !interestRate || 
        !duration || !contactPhone || !profilePhoto || !nidFrontImage || !nidBackImage) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields"
      });
    }

    // Validate numeric fields
    const numericAmount = parseFloat(amount);
    const numericInterestRate = parseFloat(interestRate);
    const numericDuration = parseInt(duration);

    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Amount must be greater than 0"
      });
    }

    if (isNaN(numericInterestRate) || numericInterestRate <= 0 || numericInterestRate > 100) {
      return res.status(400).json({
        success: false,
        message: "Interest rate must be between 0 and 100"
      });
    }

    if (isNaN(numericDuration) || numericDuration <= 0) {
      return res.status(400).json({
        success: false,
        message: "Duration must be greater than 0"
      });
    }

    // Validate NID number format
    const nidRegex = /^\d{10}$|^\d{13}$|^\d{17}$/;
    if (!nidRegex.test(nidNumber.replace(/\s/g, ''))) {
      return res.status(400).json({
        success: false,
        message: "Invalid NID number format"
      });
    }

    // Validate phone number format
    const phoneRegex = /^01[3-9]\d{8}$/;
    if (!phoneRegex.test(contactPhone)) {
      return res.status(400).json({
        success: false,
        message: "Invalid phone number format"
      });
    }

    // Validate email if provided
    if (contactEmail && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(contactEmail)) {
      return res.status(400).json({
        success: false,
        message: "Invalid email format"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId: branchId || 'main', isActive: true });
    if (!branch) {
      return res.status(400).json({
        success: false,
        message: "Invalid branch ID"
      });
    }

    // Generate unique loan ID
    const loanId = await generateLoanId(db, branch.branchCode, 'receiving');

    // Create loan document
    const newLoan = {
      loanId,
      loanDirection: 'receiving', // This is a loan receiving/application
      // Personal Profile Information
      fullName: fullName.trim(),
      fatherName: fatherName.trim(),
      motherName: motherName ? motherName.trim() : null,
      dateOfBirth: new Date(dateOfBirth),
      gender,
      maritalStatus: maritalStatus || null,
      nidNumber: nidNumber.trim().replace(/\s/g, ''),
      nidFrontImage: nidFrontImage || null,
      nidBackImage: nidBackImage || null,
      profilePhoto: profilePhoto || null,
      // Address Information
      presentAddress: presentAddress.trim(),
      permanentAddress: permanentAddress || null,
      district: district.trim(),
      upazila: upazila.trim(),
      postCode: postCode || null,
      // Business Information
      businessName: businessName || null,
      businessType: businessType || null,
      businessAddress: businessAddress || null,
      businessRegistration: businessRegistration || null,
      businessExperience: businessExperience || null,
      // Loan Details
      loanType: loanType.trim(),
      amount: numericAmount,
      source: source.trim(),
      purpose: purpose.trim(),
      interestRate: numericInterestRate,
      duration: numericDuration,
      appliedDate: new Date(appliedDate || new Date()),
      // Contact Information
      contactPerson: contactPerson || null,
      contactPhone: contactPhone.trim(),
      contactEmail: contactEmail || null,
      emergencyContact: emergencyContact || null,
      emergencyPhone: emergencyPhone || null,
      // Additional Information
      notes: notes || null,
      // Status and tracking
      status: status, // Pending by default for loan applications
      remainingAmount: numericAmount, // Initially equal to loan amount
      // Metadata
      createdBy: createdBy || 'unknown_user',
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true
    };

    // Insert loan into database
    const result = await loans.insertOne(newLoan);

    if (!result.insertedId) {
      return res.status(500).json({
        success: false,
        message: "Failed to create loan application"
      });
    }

    // Get the created loan
    const createdLoan = await loans.findOne({ _id: result.insertedId });

    // Create transaction for loan receiving if status is 'Active' (approved)
    // Note: Frontend should provide targetAccountId if loan is being approved directly
    let transactionCreated = null;
    const targetAccountId = req.body.targetAccountId; // Account where money will be credited
    
    // If loan is approved (status = 'Active') and targetAccountId is provided
    if (status === 'Active' && targetAccountId) {
      try {
        // Generate transaction ID
        const transactionId = await generateTransactionId(db, branch.branchCode);

        // Create transaction record for loan receiving
        const transactionData = {
          transactionId,
          transactionType: 'credit', // Money coming in (আমাদের account এ money আসছে)
          serviceCategory: 'Loan Receiving',
          partyType: 'loan', // New party type for loans
          partyId: result.insertedId.toString(),
          partyName: createdLoan.fullName,
          partyPhone: createdLoan.contactPhone,
          partyEmail: createdLoan.contactEmail || null,
          invoiceId: createdLoan.loanId, // Use loanId as reference
          paymentMethod: 'bank-transfer',
          targetAccountId: targetAccountId,
          accountManagerId: null,
          debitAccount: null,
          creditAccount: { id: targetAccountId },
          paymentDetails: {
            amount: numericAmount,
            reference: createdLoan.loanId,
            loanId: createdLoan.loanId,
            loanType: createdLoan.loanType
          },
          amount: numericAmount,
          branchId: branch.branchId,
          branchName: branch.branchName,
          branchCode: branch.branchCode,
          createdBy: createdBy || 'unknown_user',
          notes: `Loan Received: ${createdLoan.loanType} - ${notes || 'No additional notes'}`,
          reference: createdLoan.loanId,
          employeeReference: null,
          status: 'completed',
          date: new Date(appliedDate || new Date()),
          createdAt: new Date(),
          updatedAt: new Date(),
          isActive: true,
          loanId: createdLoan.loanId, // Link to loan document
          loanDirection: 'receiving'
        };

        // Insert transaction
        await transactions.insertOne(transactionData);
        transactionCreated = transactionId;

        // Update bank account balance (credit)
        const account = await bankAccounts.findOne({ _id: new ObjectId(targetAccountId) });
        if (account) {
          const newBalance = (account.currentBalance || 0) + numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(targetAccountId) },
            {
              $set: { currentBalance: newBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: numericAmount,
                  type: 'deposit',
                  note: `Loan Received: ${createdLoan.loanId} - ${createdLoan.fullName}`,
                  at: new Date(),
                  transactionId,
                  loanId: createdLoan.loanId
                }
              }
            }
          );
        }
      } catch (transactionError) {
        console.error('Transaction creation error for loan receiving:', transactionError);
        // Continue even if transaction creation fails - loan is already created
      }
    }

    res.status(201).json({
      success: true,
      message: status === 'Active' 
        ? "Loan has been received successfully" 
        : "Loan application has been submitted successfully",
      loan: {
        loanId: createdLoan.loanId,
        fullName: createdLoan.fullName,
        loanDirection: createdLoan.loanDirection,
        loanType: createdLoan.loanType,
        amount: createdLoan.amount,
        status: createdLoan.status,
        remainingAmount: createdLoan.remainingAmount,
        branchId: createdLoan.branchId,
        createdAt: createdLoan.createdAt
      },
      transactionId: transactionCreated || null,
      note: status === 'Pending' 
        ? "Transaction will be created when loan is approved" 
        : null
    });

  } catch (error) {
    console.error('Loan application error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while submitting loan application",
      error: error.message
    });
  }
});

// ✅ GET: Get all loans with filters
app.get("/loans", async (req, res) => {
  try {
    const {
      loanDirection, // 'giving' or 'receiving'
      status,
      branchId,
      dateFrom,
      dateTo,
      search,
      page = 1,
      limit = 20
    } = req.query;

    let filter = { isActive: true };

    // Apply filters
    if (loanDirection) {
      filter.loanDirection = loanDirection;
    }
    if (status) {
      filter.status = status;
    }
    if (branchId) {
      filter.branchId = branchId;
    }

    // Date range filter
    if (dateFrom || dateTo) {
      filter.createdAt = {};
      if (dateFrom) filter.createdAt.$gte = new Date(dateFrom);
      if (dateTo) {
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        filter.createdAt.$lte = endDate;
      }
    }

    // Search filter
    if (search) {
      filter.$or = [
        { loanId: { $regex: search, $options: 'i' } },
        { fullName: { $regex: search, $options: 'i' } },
        { nidNumber: { $regex: search, $options: 'i' } },
        { contactPhone: { $regex: search, $options: 'i' } },
        { loanType: { $regex: search, $options: 'i' } },
        { businessName: { $regex: search, $options: 'i' } }
      ];
    }

    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);

    // Get total count
    const totalCount = await loans.countDocuments(filter);

    // Get loans with pagination
    const allLoans = await loans.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .toArray();

    // Optionally add transaction count for each loan (if needed)
    // This can be slow for large lists, so make it optional
    const includeTransactionCount = req.query.includeTransactionCount === 'true';

    if (includeTransactionCount) {
      // Add transaction count for each loan
      const loansWithTransactionCount = await Promise.all(
        allLoans.map(async (loan) => {
          try {
            const txCount = await transactions.countDocuments({
              loanId: loan.loanId,
              isActive: true
            });
            return {
              ...loan,
              transactionCount: txCount
            };
          } catch (error) {
            return {
              ...loan,
              transactionCount: 0
            };
          }
        })
      );

      return res.json({
        success: true,
        count: loansWithTransactionCount.length,
        totalCount,
        currentPage: parseInt(page),
        totalPages: Math.ceil(totalCount / parseInt(limit)),
        loans: loansWithTransactionCount
      });
    }

    res.json({
      success: true,
      count: allLoans.length,
      totalCount,
      currentPage: parseInt(page),
      totalPages: Math.ceil(totalCount / parseInt(limit)),
      loans: allLoans,
      note: "Add ?includeTransactionCount=true to include transaction count for each loan"
    });

  } catch (error) {
    console.error('Get loans error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while fetching loans",
      error: error.message
    });
  }
});

// ✅ GET: Get loan by ID
app.get("/loans/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Try to find by loanId first
    let loan = await loans.findOne({
      loanId: id,
      isActive: true
    });

    // If not found by loanId, try by MongoDB ObjectId
    if (!loan && ObjectId.isValid(id)) {
      loan = await loans.findOne({
        _id: new ObjectId(id),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Get related transactions for this loan
    let relatedTransactions = [];
    let transactionCount = 0;
    let totalPaid = 0;
    let totalReceived = 0;

    try {
      // Fetch transactions related to this loan
      const transactionsList = await transactions.find({
        loanId: loan.loanId,
        isActive: true
      })
      .sort({ createdAt: -1 })
      .limit(50) // Limit to recent 50 transactions
      .toArray();

      relatedTransactions = transactionsList;
      transactionCount = transactionsList.length;

      // Calculate totals
      transactionsList.forEach(tx => {
        if (tx.transactionType === 'credit') {
          totalReceived += parseFloat(tx.amount || 0);
        } else if (tx.transactionType === 'debit') {
          totalPaid += parseFloat(tx.amount || 0);
        }
      });
    } catch (transactionError) {
      console.error('Error fetching loan transactions:', transactionError);
      // Continue without transaction data
    }

    // Prepare response with loan and transaction summary
    res.json({
      success: true,
      loan: loan,
      transactionSummary: {
        count: transactionCount,
        totalPaid: totalPaid,
        totalReceived: totalReceived,
        transactions: relatedTransactions // Include transaction list
      }
    });

  } catch (error) {
    console.error('Get loan error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while fetching loan",
      error: error.message
    });
  }
});

// ✅ POST: Record loan payment/installment (কিস্তি)
app.post("/loans/:loanId/payment", async (req, res) => {
  try {
    const { loanId } = req.params;
    const {
      amount,
      paymentDate,
      paymentMethod = 'bank-transfer',
      targetAccountId, // কোন account এ money credit হবে
      notes,
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!amount || !targetAccountId) {
      return res.status(400).json({
        success: false,
        message: "Amount and targetAccountId are required"
      });
    }

    const numericAmount = parseFloat(amount);
    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Amount must be greater than 0"
      });
    }

    // Find loan by loanId or MongoDB ObjectId
    let loan = await loans.findOne({
      loanId: loanId,
      isActive: true
    });

    if (!loan && ObjectId.isValid(loanId)) {
      loan = await loans.findOne({
        _id: new ObjectId(loanId),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Check if loan is giving type (borrower payment)
    if (loan.loanDirection !== 'giving') {
      return res.status(400).json({
        success: false,
        message: "This endpoint is only for loan giving (borrower payment). Use /loans/:loanId/repayment for loan receiving repayment."
      });
    }

    // Check if loan is active
    if (loan.status !== 'Active') {
      return res.status(400).json({
        success: false,
        message: `Cannot process payment for loan with status: ${loan.status}`
      });
    }

    // Check if payment amount exceeds remaining amount
    const currentRemaining = parseFloat(loan.remainingAmount || loan.amount || 0);
    if (numericAmount > currentRemaining) {
      return res.status(400).json({
        success: false,
        message: `Payment amount (${numericAmount}) exceeds remaining amount (${currentRemaining})`
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId: branchId || loan.branchId || 'main', isActive: true });
    if (!branch) {
      return res.status(400).json({
        success: false,
        message: "Invalid branch ID"
      });
    }

    // Calculate new remaining amount
    const newRemainingAmount = currentRemaining - numericAmount;
    const isFullyPaid = newRemainingAmount <= 0;

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Create transaction record for loan payment (CREDIT - money coming in)
    const transactionData = {
      transactionId,
      transactionType: 'credit', // Money coming in (আমাদের account এ money আসছে)
      serviceCategory: 'Loan Payment',
      partyType: 'loan',
      partyId: loan._id.toString(),
      partyName: loan.fullName,
      partyPhone: loan.contactPhone,
      partyEmail: loan.contactEmail || null,
      invoiceId: loan.loanId,
      paymentMethod: paymentMethod,
      targetAccountId: targetAccountId,
      accountManagerId: null,
      debitAccount: null,
      creditAccount: { id: targetAccountId },
      paymentDetails: {
        amount: numericAmount,
        reference: loan.loanId,
        loanId: loan.loanId,
        loanType: loan.loanType,
        paymentType: 'installment'
      },
      amount: numericAmount,
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdBy: createdBy || 'unknown_user',
      notes: `Loan Payment/Installment: ${loan.loanType} - ${notes || 'No additional notes'}`,
      reference: loan.loanId,
      employeeReference: null,
      status: 'completed',
      date: new Date(paymentDate || new Date()),
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      loanId: loan.loanId,
      loanDirection: loan.loanDirection
    };

    // Start transaction session for atomic updates
    const session = db.client.startSession();
    session.startTransaction();

    try {
      // 1. Insert transaction
      await transactions.insertOne(transactionData, { session });

      // 2. Update loan (remaining amount and status if fully paid)
      const loanUpdate = {
        remainingAmount: isFullyPaid ? 0 : newRemainingAmount,
        updatedAt: new Date()
      };

      if (isFullyPaid) {
        loanUpdate.status = 'Closed';
        loanUpdate.closedDate = new Date();
      }

      await loans.updateOne(
        { _id: loan._id },
        { $set: loanUpdate },
        { session }
      );

      // 3. Update bank account balance (credit)
      const account = await bankAccounts.findOne({ _id: new ObjectId(targetAccountId) }, { session });
      if (!account) {
        throw new Error('Bank account not found');
      }

      const newBalance = (account.currentBalance || 0) + numericAmount;
      await bankAccounts.updateOne(
        { _id: new ObjectId(targetAccountId) },
        {
          $set: { currentBalance: newBalance, updatedAt: new Date() },
          $push: {
            balanceHistory: {
              amount: numericAmount,
              type: 'deposit',
              note: `Loan Payment: ${loan.loanId} - ${loan.fullName}${isFullyPaid ? ' (Fully Paid)' : ''}`,
              at: new Date(),
              transactionId,
              loanId: loan.loanId
            }
          }
        },
        { session }
      );

      // Commit transaction
      await session.commitTransaction();

      // Get updated loan
      const updatedLoan = await loans.findOne({ _id: loan._id });

      res.status(200).json({
        success: true,
        message: isFullyPaid 
          ? "Loan payment recorded successfully. Loan is now fully paid." 
          : "Loan payment/installment recorded successfully",
        payment: {
          transactionId,
          loanId: loan.loanId,
          amount: numericAmount,
          remainingAmount: isFullyPaid ? 0 : newRemainingAmount,
          isFullyPaid,
          paymentDate: new Date(paymentDate || new Date())
        },
        loan: {
          loanId: updatedLoan.loanId,
          status: updatedLoan.status,
          remainingAmount: updatedLoan.remainingAmount,
          amount: updatedLoan.amount
        }
      });

    } catch (transactionError) {
      // Rollback transaction
      await session.abortTransaction();
      throw transactionError;
    } finally {
      await session.endSession();
    }

  } catch (error) {
    console.error('Loan payment error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while recording loan payment",
      error: error.message
    });
  }
});

// ✅ PATCH: Approve loan receiving application
app.patch("/loans/:loanId/approve", async (req, res) => {
  try {
    const { loanId } = req.params;
    const {
      targetAccountId, // কোন account এ money credit হবে
      approvedBy,
      notes
    } = req.body;

    // Validation
    if (!targetAccountId) {
      return res.status(400).json({
        success: false,
        message: "targetAccountId is required"
      });
    }

    // Find loan by loanId or MongoDB ObjectId
    let loan = await loans.findOne({
      loanId: loanId,
      isActive: true
    });

    if (!loan && ObjectId.isValid(loanId)) {
      loan = await loans.findOne({
        _id: new ObjectId(loanId),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Check if loan is receiving type
    if (loan.loanDirection !== 'receiving') {
      return res.status(400).json({
        success: false,
        message: "This endpoint is only for loan receiving applications"
      });
    }

    // Check if loan is already approved
    if (loan.status === 'Active') {
      return res.status(400).json({
        success: false,
        message: "Loan is already approved"
      });
    }

    // Check if loan is in Pending status
    if (loan.status !== 'Pending') {
      return res.status(400).json({
        success: false,
        message: `Cannot approve loan with status: ${loan.status}`
      });
    }

    // Get branch information
    const branch = await branches.findOne({ 
      branchId: loan.branchId || 'main', 
      isActive: true 
    });
    if (!branch) {
      return res.status(400).json({
        success: false,
        message: "Invalid branch ID"
      });
    }

    // Validate bank account exists
    const account = await bankAccounts.findOne({ 
      _id: new ObjectId(targetAccountId),
      isDeleted: { $ne: true }
    });
    if (!account) {
      return res.status(404).json({
        success: false,
        message: "Bank account not found"
      });
    }

    const numericAmount = parseFloat(loan.amount || 0);

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Create transaction record for loan approval (CREDIT - money coming in)
    const transactionData = {
      transactionId,
      transactionType: 'credit', // Money coming in (আমাদের account এ money আসছে)
      serviceCategory: 'Loan Receiving',
      partyType: 'loan',
      partyId: loan._id.toString(),
      partyName: loan.fullName,
      partyPhone: loan.contactPhone,
      partyEmail: loan.contactEmail || null,
      invoiceId: loan.loanId,
      paymentMethod: 'bank-transfer',
      targetAccountId: targetAccountId,
      accountManagerId: null,
      debitAccount: null,
      creditAccount: { id: targetAccountId },
      paymentDetails: {
        amount: numericAmount,
        reference: loan.loanId,
        loanId: loan.loanId,
        loanType: loan.loanType
      },
      amount: numericAmount,
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdBy: approvedBy || 'unknown_user',
      notes: `Loan Approved: ${loan.loanType} - ${notes || 'No additional notes'}`,
      reference: loan.loanId,
      employeeReference: null,
      status: 'completed',
      date: new Date(),
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      loanId: loan.loanId,
      loanDirection: 'receiving'
    };

    // Start transaction session for atomic updates
    const session = db.client.startSession();
    session.startTransaction();

    try {
      // 1. Insert transaction
      await transactions.insertOne(transactionData, { session });

      // 2. Update loan status to Active
      await loans.updateOne(
        { _id: loan._id },
        {
          $set: {
            status: 'Active',
            approvedBy: approvedBy || null,
            approvalDate: new Date(),
            updatedAt: new Date()
          }
        },
        { session }
      );

      // 3. Update bank account balance (credit)
      const newBalance = (account.currentBalance || 0) + numericAmount;
      await bankAccounts.updateOne(
        { _id: new ObjectId(targetAccountId) },
        {
          $set: { currentBalance: newBalance, updatedAt: new Date() },
          $push: {
            balanceHistory: {
              amount: numericAmount,
              type: 'deposit',
              note: `Loan Approved: ${loan.loanId} - ${loan.fullName}`,
              at: new Date(),
              transactionId,
              loanId: loan.loanId
            }
          }
        },
        { session }
      );

      // Commit transaction
      await session.commitTransaction();

      // Get updated loan
      const updatedLoan = await loans.findOne({ _id: loan._id });

      res.status(200).json({
        success: true,
        message: "Loan approved successfully",
        loan: {
          loanId: updatedLoan.loanId,
          status: updatedLoan.status,
          approvedBy: updatedLoan.approvedBy,
          approvalDate: updatedLoan.approvalDate
        },
        transactionId
      });

    } catch (transactionError) {
      // Rollback transaction
      await session.abortTransaction();
      throw transactionError;
    } finally {
      await session.endSession();
    }

  } catch (error) {
    console.error('Loan approval error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while approving loan",
      error: error.message
    });
  }
});

// ✅ POST: Record loan repayment (Loan Receiving এর জন্য)
app.post("/loans/:loanId/repayment", async (req, res) => {
  try {
    const { loanId } = req.params;
    const {
      amount,
      repaymentDate,
      paymentMethod = 'bank-transfer',
      sourceAccountId, // কোন account থেকে money debit হবে
      notes,
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!amount || !sourceAccountId) {
      return res.status(400).json({
        success: false,
        message: "Amount and sourceAccountId are required"
      });
    }

    const numericAmount = parseFloat(amount);
    if (isNaN(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Amount must be greater than 0"
      });
    }

    // Find loan by loanId or MongoDB ObjectId
    let loan = await loans.findOne({
      loanId: loanId,
      isActive: true
    });

    if (!loan && ObjectId.isValid(loanId)) {
      loan = await loans.findOne({
        _id: new ObjectId(loanId),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Check if loan is receiving type
    if (loan.loanDirection !== 'receiving') {
      return res.status(400).json({
        success: false,
        message: "This endpoint is only for loan receiving"
      });
    }

    // Check if loan is active
    if (loan.status !== 'Active') {
      return res.status(400).json({
        success: false,
        message: `Cannot process repayment for loan with status: ${loan.status}`
      });
    }

    // Check if payment amount exceeds remaining amount
    const currentRemaining = parseFloat(loan.remainingAmount || loan.amount || 0);
    if (numericAmount > currentRemaining) {
      return res.status(400).json({
        success: false,
        message: `Repayment amount (${numericAmount}) exceeds remaining amount (${currentRemaining})`
      });
    }

    // Get branch information
    const branch = await branches.findOne({ 
      branchId: branchId || loan.branchId || 'main', 
      isActive: true 
    });
    if (!branch) {
      return res.status(400).json({
        success: false,
        message: "Invalid branch ID"
      });
    }

    // Validate bank account exists and has sufficient balance
    const account = await bankAccounts.findOne({ 
      _id: new ObjectId(sourceAccountId),
      isDeleted: { $ne: true }
    });
    if (!account) {
      return res.status(404).json({
        success: false,
        message: "Bank account not found"
      });
    }

    if (account.currentBalance < numericAmount) {
      return res.status(400).json({
        success: false,
        message: "Insufficient balance in bank account"
      });
    }

    // Calculate new remaining amount
    const newRemainingAmount = currentRemaining - numericAmount;
    const isFullyRepaid = newRemainingAmount <= 0;

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Create transaction record for loan repayment (DEBIT - money going out)
    const transactionData = {
      transactionId,
      transactionType: 'debit', // Money going out (আমাদের account থেকে money বের হচ্ছে)
      serviceCategory: 'Loan Repayment',
      partyType: 'loan',
      partyId: loan._id.toString(),
      partyName: loan.fullName,
      partyPhone: loan.contactPhone,
      partyEmail: loan.contactEmail || null,
      invoiceId: loan.loanId,
      paymentMethod: paymentMethod,
      targetAccountId: sourceAccountId,
      accountManagerId: null,
      debitAccount: { id: sourceAccountId },
      creditAccount: null,
      paymentDetails: {
        amount: numericAmount,
        reference: loan.loanId,
        loanId: loan.loanId,
        loanType: loan.loanType,
        paymentType: 'repayment'
      },
      amount: numericAmount,
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      createdBy: createdBy || 'unknown_user',
      notes: `Loan Repayment: ${loan.loanType} - ${notes || 'No additional notes'}`,
      reference: loan.loanId,
      employeeReference: null,
      status: 'completed',
      date: new Date(repaymentDate || new Date()),
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      loanId: loan.loanId,
      loanDirection: 'receiving'
    };

    // Start transaction session for atomic updates
    const session = db.client.startSession();
    session.startTransaction();

    try {
      // 1. Insert transaction
      await transactions.insertOne(transactionData, { session });

      // 2. Update loan (remaining amount and status if fully repaid)
      const loanUpdate = {
        remainingAmount: isFullyRepaid ? 0 : newRemainingAmount,
        updatedAt: new Date()
      };

      if (isFullyRepaid) {
        loanUpdate.status = 'Closed';
        loanUpdate.closedDate = new Date();
      }

      await loans.updateOne(
        { _id: loan._id },
        { $set: loanUpdate },
        { session }
      );

      // 3. Update bank account balance (debit)
      const newBalance = (account.currentBalance || 0) - numericAmount;
      await bankAccounts.updateOne(
        { _id: new ObjectId(sourceAccountId) },
        {
          $set: { currentBalance: newBalance, updatedAt: new Date() },
          $push: {
            balanceHistory: {
              amount: numericAmount,
              type: 'withdrawal',
              note: `Loan Repayment: ${loan.loanId} - ${loan.fullName}${isFullyRepaid ? ' (Fully Repaid)' : ''}`,
              at: new Date(),
              transactionId,
              loanId: loan.loanId
            }
          }
        },
        { session }
      );

      // Commit transaction
      await session.commitTransaction();

      // Get updated loan
      const updatedLoan = await loans.findOne({ _id: loan._id });

      res.status(200).json({
        success: true,
        message: isFullyRepaid 
          ? "Loan repayment recorded successfully. Loan is now fully repaid." 
          : "Loan repayment recorded successfully",
        repayment: {
          transactionId,
          loanId: loan.loanId,
          amount: numericAmount,
          remainingAmount: isFullyRepaid ? 0 : newRemainingAmount,
          isFullyRepaid,
          repaymentDate: new Date(repaymentDate || new Date())
        },
        loan: {
          loanId: updatedLoan.loanId,
          status: updatedLoan.status,
          remainingAmount: updatedLoan.remainingAmount,
          amount: updatedLoan.amount
        }
      });

    } catch (transactionError) {
      // Rollback transaction
      await session.abortTransaction();
      throw transactionError;
    } finally {
      await session.endSession();
    }

  } catch (error) {
    console.error('Loan repayment error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while recording loan repayment",
      error: error.message
    });
  }
});

// ✅ PATCH: Update loan details
app.patch("/loans/:loanId", async (req, res) => {
  try {
    const { loanId } = req.params;
    const updateData = req.body;

    // Find loan by loanId or MongoDB ObjectId
    let loan = await loans.findOne({
      loanId: loanId,
      isActive: true
    });

    if (!loan && ObjectId.isValid(loanId)) {
      loan = await loans.findOne({
        _id: new ObjectId(loanId),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Don't allow updates if loan is closed
    if (loan.status === 'Closed') {
      return res.status(400).json({
        success: false,
        message: "Cannot update closed loan"
      });
    }

    // Remove fields that shouldn't be updated
    const restrictedFields = ['loanId', 'createdAt', '_id', 'loanDirection', 'amount', 'remainingAmount'];
    restrictedFields.forEach(field => {
      delete updateData[field];
    });

    // Validate numeric fields if being updated
    if (updateData.interestRate !== undefined) {
      const numericInterestRate = parseFloat(updateData.interestRate);
      if (isNaN(numericInterestRate) || numericInterestRate <= 0 || numericInterestRate > 100) {
        return res.status(400).json({
          success: false,
          message: "Interest rate must be between 0 and 100"
        });
      }
      updateData.interestRate = numericInterestRate;
    }

    if (updateData.duration !== undefined) {
      const numericDuration = parseInt(updateData.duration);
      if (isNaN(numericDuration) || numericDuration <= 0) {
        return res.status(400).json({
          success: false,
          message: "Duration must be greater than 0"
        });
      }
      updateData.duration = numericDuration;
    }

    // Add updatedAt timestamp
    updateData.updatedAt = new Date();

    // Update loan
    const result = await loans.updateOne(
      { _id: loan._id },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Get updated loan
    const updatedLoan = await loans.findOne({ _id: loan._id });

    res.json({
      success: true,
      message: "Loan updated successfully",
      loan: updatedLoan
    });

  } catch (error) {
    console.error('Update loan error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while updating loan",
      error: error.message
    });
  }
});

// ✅ PATCH: Reject loan application
app.patch("/loans/:loanId/reject", async (req, res) => {
  try {
    const { loanId } = req.params;
    const {
      rejectionReason,
      rejectedBy,
      notes
    } = req.body;

    // Find loan by loanId or MongoDB ObjectId
    let loan = await loans.findOne({
      loanId: loanId,
      isActive: true
    });

    if (!loan && ObjectId.isValid(loanId)) {
      loan = await loans.findOne({
        _id: new ObjectId(loanId),
        isActive: true
      });
    }

    if (!loan) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Check if loan is in Pending status
    if (loan.status !== 'Pending') {
      return res.status(400).json({
        success: false,
        message: `Cannot reject loan with status: ${loan.status}. Only pending loans can be rejected.`
      });
    }

    // Update loan status to Rejected
    const result = await loans.updateOne(
      { _id: loan._id },
      {
        $set: {
          status: 'Rejected',
          rejectionReason: rejectionReason || null,
          rejectedBy: rejectedBy || null,
          rejectionDate: new Date(),
          updatedAt: new Date()
        }
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: "Loan not found"
      });
    }

    // Get updated loan
    const updatedLoan = await loans.findOne({ _id: loan._id });

    res.json({
      success: true,
      message: "Loan application rejected successfully",
      loan: {
        loanId: updatedLoan.loanId,
        status: updatedLoan.status,
        rejectionReason: updatedLoan.rejectionReason,
        rejectedBy: updatedLoan.rejectedBy,
        rejectionDate: updatedLoan.rejectionDate
      }
    });

  } catch (error) {
    console.error('Loan rejection error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while rejecting loan",
      error: error.message
    });
  }
});

// ✅ GET: Vendor Analytics (Enhanced)
app.get("/vendors/analytics", async (req, res) => {
  try {
    const { period = '30' } = req.query; // Default to last 30 days
    const days = parseInt(period);
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Vendor registration trends
    const registrationTrends = await vendors.aggregate([
      {
        $match: {
          isActive: true,
          createdAt: { $gte: startDate }
        }
      },
      {
        $group: {
          _id: {
            year: { $year: "$createdAt" },
            month: { $month: "$createdAt" },
            day: { $dayOfMonth: "$createdAt" }
          },
          count: { $sum: 1 }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }
    ]).toArray();

    // Vendor demographics
    const demographics = await vendors.aggregate([
      { $match: { isActive: true } },
      {
        $group: {
          _id: null,
          withNID: { $sum: { $cond: [{ $ne: ["$nid", ""] }, 1, 0] } },
          withPassport: { $sum: { $cond: [{ $ne: ["$passport", ""] }, 1, 0] } },
          withBoth: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $ne: ["$nid", ""] },
                    { $ne: ["$passport", ""] }
                  ]
                },
                1,
                0
              ]
            }
          },
          withoutDocs: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $or: [{ $eq: ["$nid", ""] }, { $eq: ["$nid", null] }] },
                    { $or: [{ $eq: ["$passport", ""] }, { $eq: ["$passport", null] }] }
                  ]
                },
                1,
                0
              ]
            }
          }
        }
      }
    ]).toArray();

    // Top performing vendors (by order count)
    const topVendors = await orders.aggregate([
      { $match: { isActive: true } },
      {
        $group: {
          _id: "$vendorId",
          vendorName: { $first: "$vendorName" },
          orderCount: { $sum: 1 },
          totalAmount: { $sum: "$amount" },
          avgOrderValue: { $avg: "$amount" }
        }
      },
      { $sort: { orderCount: -1 } },
      { $limit: 10 }
    ]).toArray();

    res.json({
      success: true,
      analytics: {
        registrationTrends,
        demographics: demographics[0] || {},
        topVendors
      }
    });
  } catch (error) {
    console.error("Vendor analytics error:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching vendor analytics",
    });
  }
});

// ✅ GET: Order Analytics (Enhanced)
app.get("/orders/analytics", async (req, res) => {
  try {
    const { branchId, period = '30' } = req.query;
    const days = parseInt(period);
    const startDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    let filter = { isActive: true, createdAt: { $gte: startDate } };
    if (branchId) filter.branchId = branchId;

    // Order trends over time
    const orderTrends = await orders.aggregate([
      { $match: filter },
      {
        $group: {
          _id: {
            year: { $year: "$createdAt" },
            month: { $month: "$createdAt" },
            day: { $dayOfMonth: "$createdAt" }
          },
          count: { $sum: 1 },
          totalAmount: { $sum: "$amount" }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }
    ]).toArray();

    // Order status distribution
    const statusDistribution = await orders.aggregate([
      { $match: filter },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 },
          totalAmount: { $sum: "$amount" }
        }
      }
    ]).toArray();

    // Order type performance
    const typePerformance = await orders.aggregate([
      { $match: filter },
      {
        $group: {
          _id: "$orderType",
          count: { $sum: 1 },
          totalAmount: { $sum: "$amount" },
          avgAmount: { $avg: "$amount" },
          minAmount: { $min: "$amount" },
          maxAmount: { $max: "$amount" }
        }
      },
      { $sort: { totalAmount: -1 } }
    ]).toArray();

    // Revenue trends
    const revenueTrends = await orders.aggregate([
      { $match: { ...filter, status: { $in: ['confirmed', 'completed'] } } },
      {
        $group: {
          _id: {
            year: { $year: "$createdAt" },
            month: { $month: "$createdAt" },
            day: { $dayOfMonth: "$createdAt" }
          },
          revenue: { $sum: "$amount" }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }
    ]).toArray();

    // Average order processing time (for completed orders)
    const processingTime = await orders.aggregate([
      {
        $match: {
          isActive: true,
          status: 'completed',
          updatedAt: { $exists: true }
        }
      },
      {
        $addFields: {
          processingDays: {
            $divide: [
              { $subtract: ["$updatedAt", "$createdAt"] },
              1000 * 60 * 60 * 24
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          avgProcessingDays: { $avg: "$processingDays" },
          minProcessingDays: { $min: "$processingDays" },
          maxProcessingDays: { $max: "$processingDays" }
        }
      }
    ]).toArray();

    res.json({
      success: true,
      analytics: {
        orderTrends,
        statusDistribution,
        typePerformance,
        revenueTrends,
        processingTime: processingTime[0] || {}
      }
    });
  } catch (error) {
    console.error("Order analytics error:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while fetching order analytics",
    });
  }
});



// ==================== AGENT ROUTES ====================
// Create Agent
app.post("api/haj-umrah/agents", async (req, res) => {
  try {
    const {
      tradeName,
      tradeLocation,
      ownerName,
      contactNo,
      dob,
      nid,
      passport
    } = req.body;

    if (!tradeName || !tradeLocation || !ownerName || !contactNo) {
      return res.status(400).send({
        error: true,
        message: "tradeName, tradeLocation, ownerName and contactNo are required"
      });
    }

    // Basic validations similar to frontend
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(String(contactNo).trim())) {
      return res.status(400).send({ error: true, message: "Enter a valid phone number" });
    }
    if (nid && !/^[0-9]{8,20}$/.test(String(nid).trim())) {
      return res.status(400).send({ error: true, message: "NID should be 8-20 digits" });
    }
    if (passport && !/^[A-Za-z0-9]{6,12}$/.test(String(passport).trim())) {
      return res.status(400).send({ error: true, message: "Passport should be 6-12 chars" });
    }
    if (dob && !isValidDate(dob)) {
      return res.status(400).send({ error: true, message: "Invalid date format for dob (YYYY-MM-DD)" });
    }

    const now = new Date();
    const doc = {
      tradeName: String(tradeName).trim(),
      tradeLocation: String(tradeLocation).trim(),
      ownerName: String(ownerName).trim(),
      contactNo: String(contactNo).trim(),
      dob: dob || null,
      nid: nid || "",
      passport: passport || "",
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    const result = await agents.insertOne(doc);
    return res.status(201).send({
      success: true,
      message: "Agent created successfully",
      data: { _id: result.insertedId, ...doc }
    });
  } catch (error) {
    console.error('Create agent error:', error);
    res.status(500).json({ error: true, message: "Internal server error while creating agent" });
  }
});

// List Agents (with pagination and search)
app.get("api/haj-umrah/agents", async (req, res) => {
  try {
    const { page = 1, limit = 10, q } = req.query;
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 100);

    const filter = {};
    if (q && String(q).trim()) {
      const text = String(q).trim();
      filter.$or = [
        { tradeName: { $regex: text, $options: 'i' } },
        { tradeLocation: { $regex: text, $options: 'i' } },
        { ownerName: { $regex: text, $options: 'i' } },
        { contactNo: { $regex: text, $options: 'i' } },
        { nid: { $regex: text, $options: 'i' } },
        { passport: { $regex: text, $options: 'i' } }
      ];
    }

    const total = await agents.countDocuments(filter);
    const data = await agents
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Initialize due amounts if missing (migration for old agents)
    for (const agent of data) {
      if (agent.totalDue === undefined || agent.hajDue === undefined || agent.umrahDue === undefined) {
        console.log('🔄 Migrating agent to add due amounts:', agent._id);
        const updateDoc = {};
        if (agent.totalDue === undefined) updateDoc.totalDue = 0;
        if (agent.hajDue === undefined) updateDoc.hajDue = 0;
        if (agent.umrahDue === undefined) updateDoc.umrahDue = 0;
        updateDoc.updatedAt = new Date();

        await agents.updateOne(
          { _id: agent._id },
          { $set: updateDoc }
        );

        Object.assign(agent, updateDoc);
        console.log('✅ Agent migrated successfully');
      }
    }

    res.send({
      success: true,
      data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('List agents error:', error);
    res.status(500).json({ error: true, message: "Internal server error while listing agents" });
  }
});

// Get single agent by id
app.get("api/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).send({ error: true, message: "Invalid agent id" });
    }
    const agent = await agents.findOne({ _id: new ObjectId(id) });
    if (!agent) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }

    // Initialize due amounts if missing (migration for old agents)
    if (agent.totalDue === undefined || agent.hajDue === undefined || agent.umrahDue === undefined) {
      console.log('🔄 Migrating agent to add due amounts:', agent._id);
      const updateDoc = {};
      if (agent.totalDue === undefined) updateDoc.totalDue = 0;
      if (agent.hajDue === undefined) updateDoc.hajDue = 0;
      if (agent.umrahDue === undefined) updateDoc.umrahDue = 0;
      updateDoc.updatedAt = new Date();

      await agents.updateOne(
        { _id: new ObjectId(id) },
        { $set: updateDoc }
      );

      Object.assign(agent, updateDoc);
      console.log('✅ Agent migrated successfully');
    }

    res.send({ success: true, data: agent });
  } catch (error) {
    console.error('Get agent error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching agent" });
  }
});

// PUT /api/haj-umrah/agents/:id
app.put("/api/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).send({ error: true, message: "Invalid agent id" });
    }

    const {
      tradeName,
      tradeLocation,
      ownerName,
      contactNo,
      dob,
      nid,
      passport,
      isActive,
      totalDue,
      hajDue,
      umrahDue,
    } = req.body;

    // Helpers
    const isValidDateYMD = (str) => {
      if (typeof str !== "string") return false;
      if (!/^\d{4}-\d{2}-\d{2}$/.test(str)) return false;
      const d = new Date(str);
      // Ensure date is valid and preserved in UTC string (avoid 31->next month issues)
      return !Number.isNaN(d.getTime()) && str === d.toISOString().slice(0, 10);
    };

    const parseNumberField = (value, fieldName) => {
      if (value === "" || value === null) return 0; // allow empty/null as 0 if you want
      const n = typeof value === "number" ? value : parseFloat(value);
      if (!Number.isFinite(n)) {
        throw new Error(`${fieldName} must be a valid number`);
      }
      return n;
    };

    const update = { $set: { updatedAt: new Date() } };

    if (tradeName !== undefined) update.$set.tradeName = String(tradeName).trim();
    if (tradeLocation !== undefined) update.$set.tradeLocation = String(tradeLocation).trim();
    if (ownerName !== undefined) update.$set.ownerName = String(ownerName).trim();
    if (contactNo !== undefined) update.$set.contactNo = String(contactNo).trim();
    if (dob !== undefined) {
      if (dob && !isValidDateYMD(dob)) {
        return res.status(400).send({ error: true, message: "Invalid date format for dob (YYYY-MM-DD)" });
      }
      update.$set.dob = dob || null;
    }
    if (nid !== undefined) update.$set.nid = String(nid ?? "").trim();
    if (passport !== undefined) update.$set.passport = String(passport ?? "").trim();
    if (isActive !== undefined) update.$set.isActive = Boolean(isActive);
    if (totalDue !== undefined) update.$set.totalDue = parseNumberField(totalDue, "totalDue");
    if (hajDue !== undefined) update.$set.hajDue = parseNumberField(hajDue, "hajDue");
    if (umrahDue !== undefined) update.$set.umrahDue = parseNumberField(umrahDue, "umrahDue");

    // Field-level validations (only if present)
    if (update.$set.contactNo) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(update.$set.contactNo)) {
        return res.status(400).send({ error: true, message: "Enter a valid phone number" });
      }
    }
    if (update.$set.nid) {
      if (!/^[0-9]{8,20}$/.test(update.$set.nid)) {
        return res.status(400).send({ error: true, message: "NID should be 8-20 digits" });
      }
    }
    if (update.$set.passport) {
      if (!/^[A-Za-z0-9]{6,12}$/.test(update.$set.passport)) {
        return res.status(400).send({ error: true, message: "Passport should be 6-12 chars" });
      }
    }

    const result = await agents.updateOne({ _id: new ObjectId(id) }, update);
    if (result.matchedCount === 0) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }

    const updated = await agents.findOne({ _id: new ObjectId(id) });
    return res.send({ success: true, message: "Agent updated successfully", data: updated });
  } catch (err) {
    console.error("Update agent error:", err);
    const message = err?.message || "Internal server error while updating agent";
    // 400 for known validation errors, else 500
    const status = /must be a valid number|Invalid date format/i.test(message) ? 400 : 500;
    return res.status(status).json({ error: true, message });
  }
});

// ==================== HAJI ROUTES ====================
// Create Haji (customerType: 'haj')
app.post("/haj-umrah/haji", async (req, res) => {
  try {
    const data = req.body || {};

    if (!data.name || !String(data.name).trim()) {
      return res.status(400).json({ error: true, message: "Name is required" });
    }
    if (!data.mobile || !String(data.mobile).trim()) {
      return res.status(400).json({ error: true, message: "Mobile is required" });
    }

    if (data.email) {
      const emailRegex = /^\S+@\S+\.\S+$/;
      if (!emailRegex.test(String(data.email).trim())) {
        return res.status(400).json({ error: true, message: "Invalid email address" });
      }
    }

    const dateFields = ["issueDate", "expiryDate", "dateOfBirth", "departureDate", "returnDate"];
    for (const field of dateFields) {
      if (data[field]) {
        if (!isValidDate(data[field])) {
          return res.status(400).json({ error: true, message: `Invalid date format for ${field} (YYYY-MM-DD)` });
        }
      }
    }

    const now = new Date();
    // Generate unique Haji ID (reuse customer ID generator with 'haj' type)
    const hajiCustomerId = await generateCustomerId(db, 'haj');
    const doc = {
      customerId: data.customerId || hajiCustomerId,
      name: String(data.name).trim(),
      firstName: data.firstName || (String(data.name).trim().split(' ')[0] || ''),
      lastName: data.lastName || (String(data.name).trim().split(' ').slice(1).join(' ') || ''),

      mobile: String(data.mobile).trim(),
      whatsappNo: data.whatsappNo || null,
      email: data.email || null,

      address: data.address || null,
      division: data.division || null,
      district: data.district || null,
      upazila: data.upazila || null,
      postCode: data.postCode || null,

      passportNumber: data.passportNumber || data.passport || null,
      passportType: data.passportType || 'ordinary',
      issueDate: data.issueDate || null,
      expiryDate: data.expiryDate || null,
      dateOfBirth: data.dateOfBirth || null,
      nidNumber: data.nidNumber || data.nid || null,
      passportFirstName: data.passportFirstName || data.firstName || (String(data.name).trim().split(' ')[0] || ''),
      passportLastName: data.passportLastName || data.lastName || (String(data.name).trim().split(' ').slice(1).join(' ') || ''),
      nationality: data.nationality || 'Bangladeshi',
      gender: data.gender || 'male',

      fatherName: data.fatherName || null,
      motherName: data.motherName || null,
      spouseName: data.spouseName || null,
      maritalStatus: data.maritalStatus || 'single',

      occupation: data.occupation || null,
      customerImage: data.customerImage || null,
      notes: data.notes || null,
      isActive: data.isActive !== undefined ? Boolean(data.isActive) : true,

      referenceBy: data.referenceBy || null,

      serviceType: 'hajj',
      serviceStatus: data.serviceStatus || (data.paymentStatus === 'paid' ? 'confirmed' : 'pending'),

      totalAmount: Number(data.totalAmount || 0),
      paidAmount: Number(data.paidAmount || 0),
      paymentMethod: data.paymentMethod || 'cash',
      paymentStatus: (function () {
        if (data.paymentStatus) return data.paymentStatus;
        const total = Number(data.totalAmount || 0);
        const paid = Number(data.paidAmount || 0);
        if (paid >= total && total > 0) return 'paid';
        if (paid > 0 && paid < total) return 'partial';
        return 'pending';
      })(),

      packageInfo: {
        packageId: data.packageId || null,
        packageName: (data.packageInfo && data.packageInfo.packageName) || data.packageName || null,
        packageType: 'hajj',
        agentId: data.agentId || null,
        agent: (data.packageInfo && data.packageInfo.agent) || data.agent || null,
        agentContact: (data.packageInfo && data.packageInfo.agentContact) || data.agentContact || null,
        departureDate: data.departureDate || (data.packageInfo && data.packageInfo.departureDate) || null,
        returnDate: data.returnDate || (data.packageInfo && data.packageInfo.returnDate) || null,
        previousHajj: Boolean(data.previousHajj || (data.packageInfo && data.packageInfo.previousHajj)),
        previousUmrah: Boolean(data.previousUmrah || (data.packageInfo && data.packageInfo.previousUmrah)),
        specialRequirements: data.specialRequirements || (data.packageInfo && data.packageInfo.specialRequirements) || null
      },

      createdAt: now,
      updatedAt: now,
      deletedAt: null
    };

    const result = await haji.insertOne(doc);
    return res.status(201).json({ success: true, data: { _id: result.insertedId, ...doc } });
  } catch (error) {
    console.error('Create haji error:', error);
    res.status(500).json({ error: true, message: "Internal server error while creating haji" });
  }
});

// List Haji (pagination + search)
app.get("/haj-umrah/haji", async (req, res) => {
  try {
    const { page = 1, limit = 10, q, serviceStatus, paymentStatus, isActive } = req.query || {};
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 100);

    const filter = {};
    
    if (q && String(q).trim()) {
      const text = String(q).trim();
      filter.$or = [
        { name: { $regex: text, $options: 'i' } },
        { mobile: { $regex: text, $options: 'i' } },
        { email: { $regex: text, $options: 'i' } },
        { customerId: { $regex: text, $options: 'i' } },
        { passportNumber: { $regex: text, $options: 'i' } }
      ];
    }
    if (serviceStatus) filter.serviceStatus = serviceStatus;
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (isActive !== undefined) filter.isActive = String(isActive) === 'true';

    const total = await haji.countDocuments(filter);
    const data = await haji
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('List haji error:', error);
    res.status(500).json({ error: true, message: "Internal server error while listing haji" });
  }
});

// Get Haji by id or customerId
app.get("/haj-umrah/haji/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid Haji ID" });
    }

    const doc = await haji.findOne({ _id: new ObjectId(id) });

    if (!doc) {
      return res.status(404).json({ error: true, message: "Haji not found" });
    }

    const totalAmount = Number(doc?.totalAmount || 0);
    const paidAmount = Number(doc?.paidAmount || 0);
    const totalPaid = paidAmount; // alias for UI expectations
    const due = Math.max(totalAmount - paidAmount, 0);
    const hajjDue = typeof doc?.hajjDue === 'number' ? Math.max(doc.hajjDue, 0) : undefined;
    const umrahDue = typeof doc?.umrahDue === 'number' ? Math.max(doc.umrahDue, 0) : undefined;
    const normalizedPaymentStatus = (function () {
      if (paidAmount >= totalAmount && totalAmount > 0) return 'paid';
      if (paidAmount > 0 && paidAmount < totalAmount) return 'partial';
      return 'pending';
    })();
    const normalizedServiceStatus = doc?.serviceStatus || (normalizedPaymentStatus === 'paid' ? 'confirmed' : 'pending');

    res.json({
      success: true,
      data: {
        ...doc,
        totalAmount,
        paidAmount,
        totalPaid,
        due,
        paymentStatus: normalizedPaymentStatus,
        serviceStatus: normalizedServiceStatus,
        ...(typeof hajjDue === 'number' ? { hajjDue } : {}),
        ...(typeof umrahDue === 'number' ? { umrahDue } : {})
      }
    });
  } catch (error) {
    console.error('Get haji error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching haji" });
  }
});

// Update Haji by id (ObjectId)
app.put("/haj-umrah/haji/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body || {};

    // Remove fields that shouldn't be updated
    delete updates._id;
    delete updates.createdAt;

    if (updates.email) {
      const emailRegex = /^\S+@\S+\.\S+$/;
      if (!emailRegex.test(String(updates.email).trim())) {
        return res.status(400).json({ error: true, message: "Invalid email address" });
      }
    }
    const dateFields = ["issueDate", "expiryDate", "dateOfBirth", "departureDate", "returnDate"];
    for (const field of dateFields) {
      if (updates[field] && !isValidDate(updates[field])) {
        return res.status(400).json({ error: true, message: `Invalid date format for ${field} (YYYY-MM-DD)` });
      }
    }

    updates.updatedAt = new Date();

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid Haji ID" });
    }

    const result = await haji.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnDocument: 'after' }
    );

    const updatedDoc = result && (result.value || result); // support different driver return shapes
    if (!updatedDoc) {
      return res.status(404).json({ error: true, message: "Haji not found" });
    }

    res.json({ success: true, message: "Haji updated successfully", data: updatedDoc });
  } catch (error) {
    console.error('Update haji error:', error);
    res.status(500).json({ error: true, message: "Internal server error while updating haji" });
  }
});

// Delete Haji (hard delete - permanently removed from database)
app.delete("/haj-umrah/haji/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid Haji ID" });
    }

    const objectId = new ObjectId(id);

    // Check if the haji exists
    const existingHaji = await haji.findOne({ _id: objectId });

    if (!existingHaji) {
      return res.status(404).json({ error: true, message: "Haji not found" });
    }

    // Hard delete - permanently remove from database
    const result = await haji.deleteOne({ _id: objectId });

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Haji not found" });
    }

    res.json({ success: true, message: "Haji deleted successfully" });
  } catch (error) {
    console.error("Delete haji error:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting haji",
      details: error.message,
      stack: error.stack,
      receivedId: req.params.id
    });
  }
});

// ==================== UMRAH ROUTES ====================
// Create Umrah (customerType: 'umrah')
app.post("/haj-umrah/umrah", async (req, res) => {
  try {
    const data = req.body || {};

    if (!data.name || !String(data.name).trim()) {
      return res.status(400).json({ error: true, message: "Name is required" });
    }
    if (!data.mobile || !String(data.mobile).trim()) {
      return res.status(400).json({ error: true, message: "Mobile is required" });
    }

    if (data.email) {
      const emailRegex = /^\S+@\S+\.\S+$/;
      if (!emailRegex.test(String(data.email).trim())) {
        return res.status(400).json({ error: true, message: "Invalid email address" });
      }
    }

    const dateFields = ["issueDate", "expiryDate", "dateOfBirth", "departureDate", "returnDate"];
    for (const field of dateFields) {
      if (data[field]) {
        if (!isValidDate(data[field])) {
          return res.status(400).json({ error: true, message: `Invalid date format for ${field} (YYYY-MM-DD)` });
        }
      }
    }

    const now = new Date();
    // Generate unique Umrah ID (reuse customer ID generator with 'umrah' type)
    const umrahCustomerId = await generateCustomerId(db, 'umrah');
    const doc = {
      customerId: data.customerId || umrahCustomerId,
      name: String(data.name).trim(),
      firstName: data.firstName || (String(data.name).trim().split(' ')[0] || ''),
      lastName: data.lastName || (String(data.name).trim().split(' ').slice(1).join(' ') || ''),

      mobile: String(data.mobile).trim(),
      whatsappNo: data.whatsappNo || null,
      email: data.email || null,

      address: data.address || null,
      division: data.division || null,
      district: data.district || null,
      upazila: data.upazila || null,
      postCode: data.postCode || null,

      passportNumber: data.passportNumber || data.passport || null,
      passportType: data.passportType || 'ordinary',
      issueDate: data.issueDate || null,
      expiryDate: data.expiryDate || null,
      dateOfBirth: data.dateOfBirth || null,
      nidNumber: data.nidNumber || data.nid || null,
      passportFirstName: data.passportFirstName || data.firstName || (String(data.name).trim().split(' ')[0] || ''),
      passportLastName: data.passportLastName || data.lastName || (String(data.name).trim().split(' ').slice(1).join(' ') || ''),
      nationality: data.nationality || 'Bangladeshi',
      gender: data.gender || 'male',

      fatherName: data.fatherName || null,
      motherName: data.motherName || null,
      spouseName: data.spouseName || null,
      maritalStatus: data.maritalStatus || 'single',

      occupation: data.occupation || null,
      customerImage: data.customerImage || null,
      notes: data.notes || null,
      isActive: data.isActive !== undefined ? Boolean(data.isActive) : true,

      referenceBy: data.referenceBy || null,

      serviceType: 'umrah',
      serviceStatus: data.serviceStatus || (data.paymentStatus === 'paid' ? 'confirmed' : 'pending'),

      totalAmount: Number(data.totalAmount || 0),
      paidAmount: Number(data.paidAmount || 0),
      paymentMethod: data.paymentMethod || 'cash',
      paymentStatus: (function () {
        if (data.paymentStatus) return data.paymentStatus;
        const total = Number(data.totalAmount || 0);
        const paid = Number(data.paidAmount || 0);
        if (paid >= total && total > 0) return 'paid';
        if (paid > 0 && paid < total) return 'partial';
        return 'pending';
      })(),

      packageInfo: {
        packageId: data.packageId || null,
        packageName: (data.packageInfo && data.packageInfo.packageName) || data.packageName || null,
        packageType: 'umrah',
        agentId: data.agentId || null,
        agent: (data.packageInfo && data.packageInfo.agent) || data.agent || null,
        agentContact: (data.packageInfo && data.packageInfo.agentContact) || data.agentContact || null,
        departureDate: data.departureDate || (data.packageInfo && data.packageInfo.departureDate) || null,
        returnDate: data.returnDate || (data.packageInfo && data.packageInfo.returnDate) || null,
        previousHajj: Boolean(data.previousHajj || (data.packageInfo && data.packageInfo.previousHajj)),
        previousUmrah: Boolean(data.previousUmrah || (data.packageInfo && data.packageInfo.previousUmrah)),
        specialRequirements: data.specialRequirements || (data.packageInfo && data.packageInfo.specialRequirements) || null
      },

      createdAt: now,
      updatedAt: now,
      deletedAt: null
    };

    const result = await umrah.insertOne(doc);
    return res.status(201).json({ success: true, data: { _id: result.insertedId, ...doc } });
  } catch (error) {
    console.error('Create umrah error:', error);
    res.status(500).json({ error: true, message: "Internal server error while creating umrah" });
  }
});

// List Umrah (pagination + search)
app.get("/haj-umrah/umrah", async (req, res) => {
  try {
    const { page = 1, limit = 10, q, serviceStatus, paymentStatus, isActive } = req.query || {};
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 100);

    const filter = {};
    if (q && String(q).trim()) {
      const text = String(q).trim();
      filter.$or = [
        { name: { $regex: text, $options: 'i' } },
        { mobile: { $regex: text, $options: 'i' } },
        { email: { $regex: text, $options: 'i' } },
        { customerId: { $regex: text, $options: 'i' } },
        { passportNumber: { $regex: text, $options: 'i' } }
      ];
    }
    if (serviceStatus) filter.serviceStatus = serviceStatus;
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (isActive !== undefined) filter.isActive = String(isActive) === 'true';

    const total = await umrah.countDocuments(filter);
    const data = await umrah
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('List umrah error:', error);
    res.status(500).json({ error: true, message: "Internal server error while listing umrah" });
  }
});

// Get Umrah by id OR customerId (both supported)
app.get("/haj-umrah/umrah/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const isOid = ObjectId.isValid(id);
    const cond = isOid
      ? { $or: [{ _id: new ObjectId(id) }, { customerId: id }] }
      : { customerId: id };
    
    // Don't filter by isActive or deletedAt when searching by ID
    // This allows finding profiles even if they are inactive or deleted

    const doc = await umrah.findOne(cond);

    if (!doc) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    // Normalize computed fields similar to haji response
    const totalAmount = Number(doc?.totalAmount || 0);
    const paidAmount = Number(doc?.paidAmount || 0);
    const totalPaid = paidAmount; // alias for UI expectations
    const due = Math.max(totalAmount - paidAmount, 0);
    const hajjDue = typeof doc?.hajjDue === 'number' ? Math.max(doc.hajjDue, 0) : undefined;
    const umrahDue = typeof doc?.umrahDue === 'number' ? Math.max(doc.umrahDue, 0) : undefined;
    const normalizedPaymentStatus = (function () {
      if (paidAmount >= totalAmount && totalAmount > 0) return 'paid';
      if (paidAmount > 0 && paidAmount < totalAmount) return 'partial';
      return 'pending';
    })();
    const normalizedServiceStatus = doc?.serviceStatus || (normalizedPaymentStatus === 'paid' ? 'confirmed' : 'pending');

    res.json({
      success: true,
      data: {
        ...doc,
        totalAmount,
        paidAmount,
        totalPaid,
        due,
        ...(typeof hajjDue === 'number' ? { hajjDue } : {}),
        ...(typeof umrahDue === 'number' ? { umrahDue } : {}),
        paymentStatus: normalizedPaymentStatus,
        serviceStatus: normalizedServiceStatus
      }
    });
  } catch (error) {
    console.error('Get umrah error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching umrah" });
  }
});

// Recalculate Umrah paidAmount from all transactions
app.post("/haj-umrah/umrah/:id/recalculate-paid", async (req, res) => {
  try {
    const { id } = req.params;

    const isOid = ObjectId.isValid(id);
    const cond = isOid
      ? { $or: [{ _id: new ObjectId(id) }, { customerId: id }] }
      : { customerId: id };

    const doc = await umrah.findOne(cond);
    if (!doc) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    // Calculate total paidAmount from all completed credit transactions
    const allTransactions = await transactions.find({
      partyType: 'umrah',
      partyId: String(doc._id),
      transactionType: 'credit',
      status: 'completed',
      isActive: { $ne: false }
    }).toArray();

    const calculatedPaidAmount = allTransactions.reduce((sum, tx) => {
      const amount = parseFloat(tx.amount || 0);
      return sum + (isNaN(amount) ? 0 : amount);
    }, 0);

    // Clamp paidAmount: should be >= 0 and <= totalAmount
    const totalAmount = Number(doc?.totalAmount || 0);
    const finalPaidAmount = Math.max(0, Math.min(calculatedPaidAmount, totalAmount));

    // Update the profile
    await umrah.updateOne(
      { _id: doc._id },
      { 
        $set: { 
          paidAmount: finalPaidAmount, 
          updatedAt: new Date() 
        } 
      }
    );

    res.json({
      success: true,
      message: "Paid amount recalculated successfully",
      data: {
        previousPaidAmount: Number(doc?.paidAmount || 0),
        calculatedPaidAmount,
        finalPaidAmount,
        totalAmount,
        transactionCount: allTransactions.length
      }
    });
  } catch (error) {
    console.error('Recalculate umrah paidAmount error:', error);
    res.status(500).json({ error: true, message: "Internal server error while recalculating paid amount" });
  }
});

// Update Umrah by id (ObjectId)
app.put("/haj-umrah/umrah/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body || {};

    // Remove fields that shouldn't be updated
    delete updates._id;
    delete updates.createdAt;

    if (updates.email) {
      const emailRegex = /^\S+@\S+\.\S+$/;
      if (!emailRegex.test(String(updates.email).trim())) {
        return res.status(400).json({ error: true, message: "Invalid email address" });
      }
    }
    const dateFields = ["issueDate", "expiryDate", "dateOfBirth", "departureDate", "returnDate"];
    for (const field of dateFields) {
      if (updates[field] && !isValidDate(updates[field])) {
        return res.status(400).json({ error: true, message: `Invalid date format for ${field} (YYYY-MM-DD)` });
      }
    }

    updates.updatedAt = new Date();

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid Umrah ID" });
    }

    const result = await umrah.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnDocument: 'after' }
    );

    const updatedDoc = result && (result.value || result); // support different driver return shapes
    if (!updatedDoc) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    res.json({ success: true, message: "Umrah updated successfully", data: updatedDoc });
  } catch (error) {
    console.error('Update umrah error:', error);
    res.status(500).json({ error: true, message: "Internal server error while updating umrah" });
  }
});

// Delete Umrah (hard delete - permanently removed from database)
app.delete("/haj-umrah/umrah/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid Umrah ID" });
    }

    const objectId = new ObjectId(id);

    // Check if the umrah exists
    const existingUmrah = await umrah.findOne({ _id: objectId });

    if (!existingUmrah) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    // Hard delete - permanently remove from database
    const result = await umrah.deleteOne({ _id: objectId });

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    res.json({ success: true, message: "Umrah deleted successfully" });
  } catch (error) {
    console.error('Delete umrah error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while deleting umrah",
      details: error.message,
      stack: error.stack,
      receivedId: req.params.id
    });
  }
});

// ==================== AGENT PACKAGES ROUTES ====================
// Create new agent package
app.post('/api/haj-umrah/agent-packages', async (req, res) => {
  try {
    const {
      packageName,
      packageType,
      customPackageType,
      packageYear,
      totalPrice,
      duration,
      status = 'Draft',
      isActive = true,
      agentId,
      notes,
      assignedCustomers = [],
      costs,
      bangladeshAirfarePassengers = [],
      bangladeshBusPassengers = [],
      bangladeshTrainingOtherPassengers = [],
      bangladeshVisaPassengers = [],
      saudiVisaPassengers = [],
      saudiMakkahHotelPassengers = [],
      saudiMadinaHotelPassengers = [],
      saudiMakkahFoodPassengers = [],
      saudiMadinaFoodPassengers = [],
      saudiMakkahZiyaraPassengers = [],
      saudiMadinaZiyaraPassengers = [],
      saudiTransportPassengers = [],
      saudiCampFeePassengers = [],
      saudiAlMashayerPassengers = [],
      saudiOthersPassengers = [],
      totals,
      sarToBdtRate
    } = req.body;

    // Validation
    if (!packageName || !packageYear || !agentId) {
      return res.status(400).json({
        success: false,
        message: 'Package name, year, and agent ID are required'
      });
    }

    // Check if agent exists
    const agent = await agents.findOne({ _id: new ObjectId(agentId) });
    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found'
      });
    }

    // Get the grand total from the payload
    const packageTotal = totals?.grandTotal || totalPrice || 0;

    // Determine package type for due calculation
    const finalPackageType = (customPackageType || packageType || 'Regular').toLowerCase();
    const isHajjPackage = finalPackageType.includes('haj') || finalPackageType.includes('hajj');
    const isUmrahPackage = finalPackageType.includes('umrah');

    console.log('Package Type Detection:', {
      customPackageType,
      packageType,
      finalPackageType,
      isHajjPackage,
      isUmrahPackage
    });

    // Create package document
    const packageDoc = {
      packageName: String(packageName),
      packageType: packageType || 'Regular',
      customPackageType: customPackageType || '',
      packageYear: String(packageYear),
      totalPrice: packageTotal,
      duration: duration || 0,
      status: status || 'Draft',
      isActive: isActive !== false,
      agentId: new ObjectId(agentId),
      notes: notes || '',
      assignedCustomers: assignedCustomers || [],
      // Store all cost details
      costs: costs || {},
      totals: totals || {},
      sarToBdtRate: sarToBdtRate || 1,
      // Store passenger arrays
      bangladeshAirfarePassengers: bangladeshAirfarePassengers || [],
      bangladeshBusPassengers: bangladeshBusPassengers || [],
      bangladeshTrainingOtherPassengers: bangladeshTrainingOtherPassengers || [],
      bangladeshVisaPassengers: bangladeshVisaPassengers || [],
      saudiVisaPassengers: saudiVisaPassengers || [],
      saudiMakkahHotelPassengers: saudiMakkahHotelPassengers || [],
      saudiMadinaHotelPassengers: saudiMadinaHotelPassengers || [],
      saudiMakkahFoodPassengers: saudiMakkahFoodPassengers || [],
      saudiMadinaFoodPassengers: saudiMadinaFoodPassengers || [],
      saudiMakkahZiyaraPassengers: saudiMakkahZiyaraPassengers || [],
      saudiMadinaZiyaraPassengers: saudiMadinaZiyaraPassengers || [],
      saudiTransportPassengers: saudiTransportPassengers || [],
      saudiCampFeePassengers: saudiCampFeePassengers || [],
      saudiAlMashayerPassengers: saudiAlMashayerPassengers || [],
      saudiOthersPassengers: saudiOthersPassengers || [],
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Recalculate all due amounts from all existing packages first
    const existingPackages = await agentPackages.find({ agentId: new ObjectId(agentId) }).toArray();

    const result = await agentPackages.insertOne(packageDoc);

    // Now calculate from all packages (including the one we just added)
    const allPackages = [...existingPackages, packageDoc];

    let calculatedTotal = 0;
    let calculatedHajj = 0;
    let calculatedUmrah = 0;

    // Calculate totals from all packages
    for (const pkg of allPackages) {
      const pkgType = (pkg.customPackageType || pkg.packageType || 'Regular').toLowerCase();
      const pkgTotal = pkg.totalPrice || 0;
      const isPkgHajj = pkgType.includes('haj');
      const isPkgUmrah = pkgType.includes('umrah');

      calculatedTotal += pkgTotal;
      if (isPkgHajj) calculatedHajj += pkgTotal;
      if (isPkgUmrah) calculatedUmrah += pkgTotal;
    }

    console.log('Recalculated Due Amounts:', {
      total: calculatedTotal,
      haj: calculatedHajj,
      umrah: calculatedUmrah
    });

    await agents.updateOne(
      { _id: new ObjectId(agentId) },
      {
        $set: {
          totalDue: calculatedTotal,
          hajDue: calculatedHajj,
          umrahDue: calculatedUmrah,
          updatedAt: new Date()
        }
      }
    );

    // Fetch the created package with agent details
    const createdPackage = await agentPackages.findOne({ _id: result.insertedId });
    const updatedAgent = await agents.findOne({ _id: new ObjectId(agentId) });

    res.status(201).json({
      success: true,
      message: 'Package created successfully',
      data: {
        ...createdPackage,
        agent: updatedAgent
      }
    });
  } catch (error) {
    console.error('Create package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create package',
      error: error.message
    });
  }
});
// GET /api/haj-umrah/agent-packages
// Get all agent packages with filters
app.get('/api/haj-umrah/agent-packages', async (req, res) => {
  try {
    const { agentId, year, type, limit = 10, page = 1 } = req.query;

    const filter = {};
    if (agentId) filter.agentId = new ObjectId(agentId);
    if (year) filter.packageYear = String(year);
    if (type) filter.packageType = type;

    const limitNum = parseInt(limit);
    const pageNum = parseInt(page);

    const packages = await agentPackages
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Populate agent information
    const packagesWithAgents = await Promise.all(
      packages.map(async (pkg) => {
        if (pkg.agentId) {
          const agent = await agents.findOne({ _id: pkg.agentId });
          return { ...pkg, agent };
        }
        return pkg;
      })
    );

    const total = await agentPackages.countDocuments(filter);

    res.json({
      success: true,
      data: packagesWithAgents,
      pagination: {
        total,
        page: pageNum,
        limit: limitNum,
        pages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get packages error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch agent packages',
      error: error.message
    });
  }
});

// GET /api/haj-umrah/agent-packages/:id
// Get single agent package
app.get('/api/haj-umrah/agent-packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const package = await agentPackages.findOne({ _id: new ObjectId(id) });

    if (!package) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    // Populate agent information
    if (package.agentId) {
      const agent = await agents.findOne({ _id: package.agentId });
      package.agent = agent;
    }

    res.json({
      success: true,
      data: package
    });
  } catch (error) {
    console.error('Get package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch package',
      error: error.message
    });
  }
});


// PUT /api/haj-umrah/agent-packages/:id
// Update agent package
app.put('/api/haj-umrah/agent-packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const updateData = {
      ...req.body,
      updatedAt: new Date()
    };

    const result = await agentPackages.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const updatedPackage = await agentPackages.findOne({ _id: new ObjectId(id) });

    // Populate agent information
    if (updatedPackage.agentId) {
      const agent = await agents.findOne({ _id: updatedPackage.agentId });
      updatedPackage.agent = agent;
    }

    res.json({
      success: true,
      message: 'Package updated successfully',
      data: updatedPackage
    });
  } catch (error) {
    console.error('Update package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update package',
      error: error.message
    });
  }
});

// DELETE /api/haj-umrah/agent-packages/:id
// Delete agent package
app.delete('/api/haj-umrah/agent-packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const result = await agentPackages.deleteOne({ _id: new ObjectId(id) });

    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    res.json({
      success: true,
      message: 'Package deleted successfully'
    });
  } catch (error) {
    console.error('Delete package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete package',
      error: error.message
    });
  }
});

// POST /api/haj-umrah/agent-packages/:id/assign-customers
// Assign customers/pilgrims to package
app.post('/api/haj-umrah/agent-packages/:id/assign-customers', async (req, res) => {
  try {
    const { id } = req.params;
    const { customerIds, pilgrimData } = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Check if package exists
    const package = await agentPackages.findOne({ _id: new ObjectId(id) });
    if (!package) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const existingCustomers = package.assignedCustomers || [];

    // Handle array of customer IDs
    if (customerIds && Array.isArray(customerIds)) {
      const existingIds = existingCustomers.map(c => c._id?.toString() || c.toString());
      const newIds = customerIds
        .map(id => new ObjectId(id))
        .filter(id => !existingIds.includes(id.toString()));

      const updatedCustomers = [...existingCustomers, ...newIds];

      await agentPackages.updateOne(
        { _id: new ObjectId(id) },
        {
          $set: {
            assignedCustomers: updatedCustomers,
            updatedAt: new Date()
          }
        }
      );

      return res.json({
        success: true,
        message: `${newIds.length} customers assigned successfully`
      });
    }

    // Handle pilgrim data object (for haji/umrah)
    if (pilgrimData) {
      const newPilgrim = {
        _id: new ObjectId(),
        ...pilgrimData,
        assignedAt: new Date()
      };

      const updatedCustomers = [...existingCustomers, newPilgrim];

      await agentPackages.updateOne(
        { _id: new ObjectId(id) },
        {
          $set: {
            assignedCustomers: updatedCustomers,
            updatedAt: new Date()
          }
        }
      );

      return res.json({
        success: true,
        message: 'Pilgrim assigned successfully',
        data: newPilgrim
      });
    }

    res.status(400).json({
      success: false,
      message: 'Either customerIds array or pilgrimData object is required'
    });
  } catch (error) {
    console.error('Assign customer error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to assign customers',
      error: error.message
    });
  }
});

// DELETE /api/haj-umrah/agent-packages/:id/remove-customer/:customerId
// Remove customer from package
app.delete('/api/haj-umrah/agent-packages/:id/remove-customer/:customerId', async (req, res) => {
  try {
    const { id, customerId } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const package = await agentPackages.findOne({ _id: new ObjectId(id) });
    if (!package) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const updatedCustomers = (package.assignedCustomers || []).filter(
      c => c._id?.toString() !== customerId && c.toString() !== customerId
    );

    await agentPackages.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          assignedCustomers: updatedCustomers,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: 'Customer removed from package'
    });
  } catch (error) {
    console.error('Remove customer error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to remove customer',
      error: error.message
    });
  }
});


// GET /haj-umrah/agents
// Get all agents
app.get('/api/haj-umrah/agents', async (req, res) => {
  try {
    const { page = 1, limit = 100, search = '' } = req.query;

    const filter = {};
    if (search) {
      filter.$or = [
        { tradeName: { $regex: search, $options: 'i' } },
        { ownerName: { $regex: search, $options: 'i' } },
        { contactNo: { $regex: search, $options: 'i' } }
      ];
    }

    const limitNum = parseInt(limit);
    const pageNum = parseInt(page);

    const agentsList = await agents
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Initialize due amounts if missing (migration for old agents)
    for (const agent of agentsList) {
      if (agent.totalDue === undefined || agent.hajDue === undefined || agent.umrahDue === undefined) {
        console.log('🔄 Migrating agent to add due amounts:', agent._id);
        const updateDoc = {};
        if (agent.totalDue === undefined) updateDoc.totalDue = 0;
        if (agent.hajDue === undefined) updateDoc.hajDue = 0;
        if (agent.umrahDue === undefined) updateDoc.umrahDue = 0;
        updateDoc.updatedAt = new Date();

        await agents.updateOne(
          { _id: agent._id },
          { $set: updateDoc }
        );

        Object.assign(agent, updateDoc);
        console.log('✅ Agent migrated successfully');
      }
    }

    const total = await agents.countDocuments(filter);

    res.json({
      success: true,
      data: agentsList,
      pagination: {
        total,
        page: pageNum,
        limit: limitNum,
        pages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get agents error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch agents',
      error: error.message
    });
  }
});

// GET /api/haj-umrah/agents/:id
// Get single agent with packages
app.get('/api/haj-umrah/agents/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid agent ID'
      });
    }

    const agent = await agents.findOne({ _id: new ObjectId(id) });

    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found'
      });
    }

    // Initialize due amounts if missing (migration for old agents)
    if (agent.totalDue === undefined || agent.hajDue === undefined || agent.umrahDue === undefined) {
      console.log('🔄 Migrating agent to add due amounts:', agent._id);
      const updateDoc = {};
      if (agent.totalDue === undefined) updateDoc.totalDue = 0;
      if (agent.hajDue === undefined) updateDoc.hajDue = 0;
      if (agent.umrahDue === undefined) updateDoc.umrahDue = 0;
      updateDoc.updatedAt = new Date();

      await agents.updateOne(
        { _id: new ObjectId(id) },
        { $set: updateDoc }
      );

      Object.assign(agent, updateDoc);
      console.log('✅ Agent migrated successfully');
    }

    // Get all packages for this agent only
    const packages = await agentPackages
      .find({ agentId: new ObjectId(id) })
      .sort({ createdAt: -1 })
      .toArray();

    // Format response with agent info and their packages
    const response = {
      ...agent,
      packages: packages.map(pkg => ({
        _id: pkg._id,
        packageName: pkg.packageName,
        packageType: pkg.packageType,
        customPackageType: pkg.customPackageType,
        packageYear: pkg.packageYear,
        totalPrice: pkg.totalPrice,
        status: pkg.status,
        isActive: pkg.isActive,
        createdAt: pkg.createdAt,
        updatedAt: pkg.updatedAt,
        // Include basic cost info if needed
        totals: pkg.totals ? {
          grandTotal: pkg.totals.grandTotal,
          subtotal: pkg.totals.subtotal
        } : null
      })),
      packageCount: packages.length,
      summary: {
        totalDue: agent.totalDue || 0,
        hajDue: agent.hajDue || 0,
        umrahDue: agent.umrahDue || 0,
        totalPackages: packages.length,
        activePackages: packages.filter(p => p.isActive !== false).length
      }
    };

    res.json({
      success: true,
      data: response
    });
  } catch (error) {
    console.error('Get agent error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch agent',
      error: error.message
    });
  }
});


// ==================== PACKAGES ROUTES (for general package management without agents) ====================
// POST /haj-umrah/packages - Create new package
app.post('/haj-umrah/packages', async (req, res) => {
  try {
    const {
      packageName,
      packageYear,
      packageMonth,
      packageType,
      customPackageType,
      sarToBdtRate,
      notes,
      costs,
      totals,
      status
    } = req.body;

    // Validation
    if (!packageName || !packageYear) {
      return res.status(400).json({
        success: false,
        message: 'Package name and year are required'
      });
    }

    // Ensure totals.passengerTotals structure exists
    const totalsData = totals || {};
    if (!totalsData.passengerTotals) {
      totalsData.passengerTotals = {
        adult: 0,
        child: 0,
        infant: 0
      };
    } else {
      // Ensure all three passenger types are present
      totalsData.passengerTotals = {
        adult: parseFloat(totalsData.passengerTotals.adult) || 0,
        child: parseFloat(totalsData.passengerTotals.child) || 0,
        infant: parseFloat(totalsData.passengerTotals.infant) || 0
      };
    }

    // Create package document
    const packageDoc = {
      packageName: String(packageName),
      packageYear: String(packageYear),
      packageMonth: packageMonth || '',
      packageType: packageType || 'Regular',
      customPackageType: customPackageType || '',
      sarToBdtRate: parseFloat(sarToBdtRate) || 0,
      notes: notes || '',
      status: status || 'Active',
      costs: costs || {},
      totals: totalsData,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await packages.insertOne(packageDoc);
    const createdPackage = await packages.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: 'Package created successfully',
      data: createdPackage
    });
  } catch (error) {
    console.error('Create package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create package',
      error: error.message
    });
  }
});

// GET /haj-umrah/packages - Get all packages
app.get('/haj-umrah/packages', async (req, res) => {
  try {
    const { year, month, type, customPackageType, status, limit = 50, page = 1 } = req.query;

    const filter = {};
    if (year) filter.packageYear = String(year);
    if (month) filter.packageMonth = String(month);
    if (type) filter.packageType = type;
    if (customPackageType) filter.customPackageType = customPackageType;
    if (status) filter.status = status;

    const limitNum = parseInt(limit);
    const pageNum = parseInt(page);

    const packagesList = await packages
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Ensure passengerTotals structure exists in all packages
    const normalizedPackages = packagesList.map(pkg => {
      if (pkg.totals && !pkg.totals.passengerTotals) {
        pkg.totals.passengerTotals = {
          adult: 0,
          child: 0,
          infant: 0
        };
      } else if (!pkg.totals) {
        pkg.totals = {
          passengerTotals: {
            adult: 0,
            child: 0,
            infant: 0
          }
        };
      } else if (pkg.totals.passengerTotals) {
        // Ensure all three passenger types are present
        pkg.totals.passengerTotals = {
          adult: parseFloat(pkg.totals.passengerTotals.adult) || 0,
          child: parseFloat(pkg.totals.passengerTotals.child) || 0,
          infant: parseFloat(pkg.totals.passengerTotals.infant) || 0
        };
      }
      return pkg;
    });

    const total = await packages.countDocuments(filter);

    res.json({
      success: true,
      data: normalizedPackages,
      pagination: {
        total,
        page: pageNum,
        limit: limitNum,
        pages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get packages error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch packages',
      error: error.message
    });
  }
});

// GET /haj-umrah/packages/:id - Get single package
app.get('/haj-umrah/packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const package = await packages.findOne({ _id: new ObjectId(id) });

    if (!package) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    // Ensure passengerTotals structure exists
    if (package.totals && !package.totals.passengerTotals) {
      package.totals.passengerTotals = {
        adult: 0,
        child: 0,
        infant: 0
      };
    } else if (!package.totals) {
      package.totals = {
        passengerTotals: {
          adult: 0,
          child: 0,
          infant: 0
        }
      };
    } else if (package.totals.passengerTotals) {
      // Ensure all three passenger types are present
      package.totals.passengerTotals = {
        adult: parseFloat(package.totals.passengerTotals.adult) || 0,
        child: parseFloat(package.totals.passengerTotals.child) || 0,
        infant: parseFloat(package.totals.passengerTotals.infant) || 0
      };
    }

    res.json({
      success: true,
      data: package
    });
  } catch (error) {
    console.error('Get package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch package',
      error: error.message
    });
  }
});

// PUT /haj-umrah/packages/:id - Update package
app.put('/haj-umrah/packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Get existing package to preserve structure
    const existingPackage = await packages.findOne({ _id: new ObjectId(id) });
    if (!existingPackage) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const updateData = {
      ...req.body,
      updatedAt: new Date()
    };

    // Ensure totals.passengerTotals structure exists if totals is being updated
    if (updateData.totals) {
      if (!updateData.totals.passengerTotals) {
        // If passengerTotals not provided, check if existing package has it
        const existingPassengerTotals = existingPackage.totals?.passengerTotals;
        if (existingPassengerTotals) {
          updateData.totals.passengerTotals = existingPassengerTotals;
        } else {
          updateData.totals.passengerTotals = {
            adult: 0,
            child: 0,
            infant: 0
          };
        }
      } else {
        // Ensure all three passenger types are present
        updateData.totals.passengerTotals = {
          adult: parseFloat(updateData.totals.passengerTotals.adult) || 0,
          child: parseFloat(updateData.totals.passengerTotals.child) || 0,
          infant: parseFloat(updateData.totals.passengerTotals.infant) || 0
        };
      }
    }

    const result = await packages.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const updatedPackage = await packages.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: 'Package updated successfully',
      data: updatedPackage
    });
  } catch (error) {
    console.error('Update package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update package',
      error: error.message
    });
  }
});

// DELETE /haj-umrah/packages/:id - Delete package
app.delete('/haj-umrah/packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const result = await packages.deleteOne({ _id: new ObjectId(id) });

    if (result.deletedCount === 0) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    res.json({
      success: true,
      message: 'Package deleted successfully'
    });
  } catch (error) {
    console.error('Delete package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete package',
      error: error.message
    });
  }
});

// POST /haj-umrah/packages/:id/assign-passenger - Assign package to passenger with type selection
app.post('/haj-umrah/packages/:id/assign-passenger', async (req, res) => {
  try {
    const { id } = req.params;
    const { passengerId, passengerType, passengerCategory } = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Validation
    if (!passengerId) {
      return res.status(400).json({
        success: false,
        message: 'Passenger ID is required'
      });
    }

    if (!passengerType || !['adult', 'child', 'infant'].includes(passengerType.toLowerCase())) {
      return res.status(400).json({
        success: false,
        message: 'Passenger type must be one of: adult, child, infant'
      });
    }

    // Get package
    const package = await packages.findOne({ _id: new ObjectId(id) });
    if (!package) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    // Get passengerTotals from package
    const passengerTotals = package.totals?.passengerTotals || {};
    const passengerTypeKey = passengerType.toLowerCase();
    const selectedPrice = parseFloat(passengerTotals[passengerTypeKey]) || 0;

    if (selectedPrice <= 0) {
      return res.status(400).json({
        success: false,
        message: `No price available for passenger type: ${passengerType}`
      });
    }

    // Determine which collection to update (haji or umrah)
    const isHajjPackage = package.customPackageType?.toLowerCase().includes('hajj') || 
                         package.packageType?.toLowerCase().includes('hajj');
    const isUmrahPackage = package.customPackageType?.toLowerCase().includes('umrah') || 
                          package.packageType?.toLowerCase().includes('umrah');

    // Use passengerCategory if provided, otherwise infer from package
    let targetCollection = null;
    let collectionName = '';

    if (passengerCategory) {
      if (passengerCategory.toLowerCase() === 'haji' || passengerCategory.toLowerCase() === 'hajj') {
        targetCollection = haji;
        collectionName = 'haji';
      } else if (passengerCategory.toLowerCase() === 'umrah') {
        targetCollection = umrah;
        collectionName = 'umrah';
      }
    } else {
      if (isHajjPackage) {
        targetCollection = haji;
        collectionName = 'haji';
      } else if (isUmrahPackage) {
        targetCollection = umrah;
        collectionName = 'umrah';
      }
    }

    if (!targetCollection) {
      return res.status(400).json({
        success: false,
        message: 'Could not determine passenger category. Please specify passengerCategory (haji/umrah) or ensure package has valid type.'
      });
    }

    // Find passenger
    let passenger = null;
    const passengerObjId = ObjectId.isValid(passengerId) ? new ObjectId(passengerId) : null;
    
    if (passengerObjId) {
      passenger = await targetCollection.findOne({ 
        $or: [
          { _id: passengerObjId },
          { customerId: passengerId }
        ]
      });
    } else {
      passenger = await targetCollection.findOne({ customerId: passengerId });
    }

    if (!passenger) {
      return res.status(404).json({
        success: false,
        message: `Passenger not found in ${collectionName} collection`
      });
    }

    // Update passenger profile with package information
    const updateData = {
      $set: {
        totalAmount: selectedPrice,
        packageInfo: {
          packageId: new ObjectId(id),
          packageName: package.packageName,
          packageType: package.packageType || 'Regular',
          customPackageType: package.customPackageType || '',
          packageYear: package.packageYear,
          packageMonth: package.packageMonth || '',
          passengerType: passengerTypeKey,
          passengerPrice: selectedPrice,
          assignedAt: new Date()
        },
        updatedAt: new Date()
      }
    };

    // If paidAmount doesn't exist or is 0, keep it at current value, otherwise preserve it
    const currentPaidAmount = passenger.paidAmount || 0;
    if (!passenger.paidAmount && passenger.paidAmount !== 0) {
      updateData.$set.paidAmount = 0;
    }

    // Update payment status based on paid vs total
    const finalTotal = selectedPrice;
    const finalPaid = passenger.paidAmount || 0;
    if (finalPaid >= finalTotal && finalTotal > 0) {
      updateData.$set.paymentStatus = 'paid';
    } else if (finalPaid > 0 && finalPaid < finalTotal) {
      updateData.$set.paymentStatus = 'partial';
    } else {
      updateData.$set.paymentStatus = 'pending';
    }

    await targetCollection.updateOne(
      { _id: passenger._id },
      updateData
    );

    const updatedPassenger = await targetCollection.findOne({ _id: passenger._id });

    res.json({
      success: true,
      message: `Package assigned successfully to ${passengerType}`,
      data: {
        passenger: updatedPassenger,
        package: {
          _id: package._id,
          packageName: package.packageName,
          passengerType: passengerTypeKey,
          price: selectedPrice
        }
      }
    });
  } catch (error) {
    console.error('Assign passenger to package error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to assign passenger to package',
      error: error.message
    });
  }
});

// ==================== BANK ACCOUNTS ROUTES ====================
// Schema (MongoDB):
// {
//   bankName, accountNumber, accountType, accountCategory, branchName, accountHolder, accountTitle,
//   initialBalance, currentBalance, currency, contactNumber, logo, createdBy, branchId,
//   status: 'Active'|'Inactive', createdAt, updatedAt, isDeleted, balanceHistory?
// }

// Create bank account
app.post("/bank-accounts", async (req, res) => {
  try {
    const {
      bankName,
      accountNumber,
      accountType = "Current",
      accountCategory = "bank", // New field with default value
      branchName,
      accountHolder,
      accountTitle,
      initialBalance,
      currency = "BDT",
      contactNumber,
      logo,
      createdBy, // New field
      branchId // New field
    } = req.body || {};

    // Updated validation to include new required fields
    if (!bankName || !accountNumber || !accountType || !accountCategory || !branchName || !accountHolder || !accountTitle || initialBalance === undefined) {
      return res.status(400).json({ success: false, error: "Missing required fields" });
    }

    const numericInitial = Number(initialBalance);
    if (!Number.isFinite(numericInitial) || numericInitial < 0) {
      return res.status(400).json({ success: false, error: "Invalid initialBalance" });
    }

    // Validate account category
    const validCategories = ['cash', 'bank', 'mobile_banking', 'check', 'others'];
    if (!validCategories.includes(accountCategory)) {
      return res.status(400).json({ success: false, error: "Invalid account category" });
    }

    // Validate contact number format if provided
    if (contactNumber && !/^[\+]?[0-9\s\-\(\)]+$/.test(contactNumber)) {
      return res.status(400).json({ success: false, error: "Invalid contact number format" });
    }

    // Ensure unique accountNumber per currency
    const existing = await bankAccounts.findOne({ accountNumber, currency, isDeleted: { $ne: true } });
    if (existing) {
      return res.status(409).json({ success: false, error: "Account with this number already exists" });
    }

    const doc = {
      bankName,
      accountNumber,
      accountType,
      accountCategory, // New field
      branchName,
      accountHolder,
      accountTitle,
      initialBalance: numericInitial,
      currentBalance: numericInitial,
      currency,
      contactNumber: contactNumber || null,
      logo: logo || null,
      createdBy: createdBy || null, // New field
      branchId: branchId || null, // New field
      status: "Active",
      isDeleted: false,
      createdAt: new Date(),
      updatedAt: new Date(),
      balanceHistory: []
    };

    const result = await bankAccounts.insertOne(doc);
    return res.json({ success: true, data: { _id: result.insertedId, ...doc } });
  } catch (error) {
    console.error("❌ Error creating bank account:", error);
    res.status(500).json({ success: false, error: "Failed to create bank account" });
  }
});

// Get all bank accounts with optional query filters
app.get("/bank-accounts", async (req, res) => {
  try {
    const { status, accountType, accountCategory, currency, search } = req.query || {};
    const query = { isDeleted: { $ne: true } };
    if (status) query.status = status;
    if (accountType) query.accountType = accountType;
    if (accountCategory) query.accountCategory = accountCategory;
    if (currency) query.currency = currency;
    if (search) {
      query.$or = [
        { bankName: { $regex: search, $options: "i" } },
        { accountNumber: { $regex: search, $options: "i" } },
        { branchName: { $regex: search, $options: "i" } },
        { accountHolder: { $regex: search, $options: "i" } },
        { accountTitle: { $regex: search, $options: "i" } }
      ];
    }
    const data = await bankAccounts.find(query).sort({ createdAt: -1 }).toArray();
    res.json({ success: true, data });
  } catch (error) {
    console.error("❌ Error fetching bank accounts:", error);
    res.status(500).json({ success: false, error: "Failed to fetch bank accounts" });
  }
});

// Get single bank account
app.get("/bank-accounts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const account = await bankAccounts.findOne({ _id: new ObjectId(id), isDeleted: { $ne: true } });
    if (!account) return res.status(404).json({ success: false, error: "Bank account not found" });
    res.json({ success: true, data: account });
  } catch (error) {
    console.error("❌ Error getting bank account:", error);
    res.status(500).json({ success: false, error: "Failed to get bank account" });
  }
});

// Update bank account
app.put("/bank-accounts/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ success: false, error: "Invalid bank account ID format" });
    }

    const update = { ...req.body };

    if (update.initialBalance !== undefined) {
      const numeric = Number(update.initialBalance);
      if (!Number.isFinite(numeric) || numeric < 0) {
        return res.status(400).json({ success: false, error: "Invalid initialBalance" });
      }
      update.initialBalance = numeric;
    }

    // Validate account category if provided
    if (update.accountCategory) {
      const validCategories = ['cash', 'bank', 'mobile_banking', 'check', 'others'];
      if (!validCategories.includes(update.accountCategory)) {
        return res.status(400).json({ success: false, error: "Invalid account category" });
      }
    }

    // Validate contact number format if provided
    if (update.contactNumber && !/^[\+]?[0-9\s\-\(\)]+$/.test(update.contactNumber)) {
      return res.status(400).json({ success: false, error: "Invalid contact number format" });
    }

    if (update.accountNumber || update.currency) {
      const toCheckNumber = update.accountNumber;
      const toCheckCurrency = update.currency;
      if (toCheckNumber || toCheckCurrency) {
        const current = await bankAccounts.findOne({ _id: new ObjectId(id) });
        if (!current || current.isDeleted) {
          return res.status(404).json({ success: false, error: "Bank account not found" });
        }
        const number = toCheckNumber || current.accountNumber;
        const curr = toCheckCurrency || current.currency;
        const existing = await bankAccounts.findOne({ _id: { $ne: new ObjectId(id) }, accountNumber: number, currency: curr, isDeleted: { $ne: true } });
        if (existing) {
          return res.status(409).json({ success: false, error: "Account with this number already exists" });
        }
      }
    }

    update.updatedAt = new Date();
    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id), isDeleted: { $ne: true } },
      { $set: update },
      { returnDocument: "after" }
    );

    // ✅ FIXED: Changed from result.value to result
    if (!result) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }
    res.json({ success: true, data: result });
  } catch (error) {
    console.error("❌ Error updating bank account:", error);
    res.status(500).json({ success: false, error: "Failed to update bank account" });
  }
});

// Soft delete bank account
app.delete("/bank-accounts/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ success: false, error: "Invalid bank account ID format" });
    }

    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id), isDeleted: { $ne: true } },
      { $set: { isDeleted: true, status: "Inactive", updatedAt: new Date() } },
      { returnDocument: "after" }
    );

    // ✅ FIXED: Changed from result.value to result
    if (!result) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }
    res.json({ success: true, data: result });
  } catch (error) {
    console.error("❌ Error deleting bank account:", error);
    res.status(500).json({ success: false, error: "Failed to delete bank account" });
  }
});

// Balance adjustment
app.post("/bank-accounts/:id/adjust-balance", async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, type, note, createdBy, branchId } = req.body || {};

    const numericAmount = Number(amount);
    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({ success: false, error: "Invalid amount" });
    }
    if (!type || !["deposit", "withdrawal"].includes(type)) {
      return res.status(400).json({ success: false, error: "Invalid transaction type" });
    }

    const account = await bankAccounts.findOne({ _id: new ObjectId(id), isDeleted: { $ne: true } });
    if (!account) return res.status(404).json({ success: false, error: "Bank account not found" });

    let newBalance = account.currentBalance;
    if (type === "deposit") newBalance += numericAmount;
    else newBalance -= numericAmount;
    if (newBalance < 0) {
      return res.status(400).json({ success: false, error: "Insufficient balance" });
    }

    // Get branch information for transaction
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).json({ success: false, error: "Invalid branch ID" });
    }

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Create transaction record
    const transactionRecord = {
      transactionId,
      transactionType: type === "deposit" ? "credit" : "debit",
      customerId: "SYSTEM",
      customerName: "System",
      customerPhone: null,
      customerEmail: null,
      category: "Bank Balance Adjustment",
      paymentMethod: "bank-transfer",
      paymentDetails: {
        bankName: account.bankName,
        accountNumber: account.accountNumber,
        amount: numericAmount,
        reference: `Balance ${type} - ${note || 'No note provided'}`
      },
      customerBankAccount: {
        bankName: account.bankName,
        accountNumber: account.accountNumber
      },
      notes: note || `Bank account balance ${type}`,
      date: new Date(),
      createdBy: createdBy || "SYSTEM",
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      status: 'completed',
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      bankAccountId: account._id,
      isBankTransaction: true
    };

    // Update bank account balance and create transaction in a single operation
    const update = {
      currentBalance: newBalance,
      updatedAt: new Date()
    };

    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: update, $push: { balanceHistory: { amount: numericAmount, type, note: note || null, at: new Date(), transactionId } } },
      { returnDocument: "after" }
    );

    // Insert transaction record
    await transactions.insertOne(transactionRecord);

    res.json({ success: true, data: result.value });
  } catch (error) {
    console.error("❌ Error adjusting balance:", error);
    res.status(500).json({ success: false, error: "Failed to adjust balance" });
  }
});

// Bank stats overview
app.get("/bank-accounts/stats/overview", async (req, res) => {
  try {
    const pipeline = [
      { $match: { isDeleted: { $ne: true } } },
      {
        $group: {
          _id: null,
          totalAccounts: { $sum: 1 },
          totalBalance: { $sum: "$currentBalance" },
          totalInitialBalance: { $sum: "$initialBalance" },
          activeAccounts: { $sum: { $cond: [{ $eq: ["$status", "Active"] }, 1, 0] } },
          bankAccounts: { $sum: { $cond: [{ $eq: ["$accountCategory", "bank"] }, 1, 0] } },
          cashAccounts: { $sum: { $cond: [{ $eq: ["$accountCategory", "cash"] }, 1, 0] } },
          mobileBankingAccounts: { $sum: { $cond: [{ $eq: ["$accountCategory", "mobile_banking"] }, 1, 0] } },
          checkAccounts: { $sum: { $cond: [{ $eq: ["$accountCategory", "check"] }, 1, 0] } },
          otherAccounts: { $sum: { $cond: [{ $eq: ["$accountCategory", "others"] }, 1, 0] } }
        }
      }
    ];
    const stats = await bankAccounts.aggregate(pipeline).toArray();
    const data = stats[0] || {
      totalAccounts: 0,
      totalBalance: 0,
      totalInitialBalance: 0,
      activeAccounts: 0,
      bankAccounts: 0,
      cashAccounts: 0,
      mobileBankingAccounts: 0,
      checkAccounts: 0,
      otherAccounts: 0
    };
    res.json({ success: true, data });
  } catch (error) {
    console.error("❌ Error getting bank stats:", error);
    res.status(500).json({ success: false, error: "Failed to get bank stats" });
  }
});

// Get bank accounts by category
app.get("/bank-accounts/category/:category", async (req, res) => {
  try {
    const { category } = req.params;
    const { status, currency, search } = req.query || {};

    // Validate category
    const validCategories = ['cash', 'bank', 'mobile_banking', 'check', 'others'];
    if (!validCategories.includes(category)) {
      return res.status(400).json({ success: false, error: "Invalid account category" });
    }

    const query = {
      isDeleted: { $ne: true },
      accountCategory: category
    };

    if (status) query.status = status;
    if (currency) query.currency = currency;
    if (search) {
      query.$or = [
        { bankName: { $regex: search, $options: "i" } },
        { accountNumber: { $regex: search, $options: "i" } },
        { branchName: { $regex: search, $options: "i" } },
        { accountHolder: { $regex: search, $options: "i" } },
        { accountTitle: { $regex: search, $options: "i" } }
      ];
    }

    const data = await bankAccounts.find(query).sort({ createdAt: -1 }).toArray();
    res.json({ success: true, data });
  } catch (error) {
    console.error("❌ Error fetching bank accounts by category:", error);
    res.status(500).json({ success: false, error: "Failed to fetch bank accounts by category" });
  }
});

// Get bank account transaction history
app.get("/bank-accounts/:id/transactions", async (req, res) => {
  try {
    const { id } = req.params;
    const { page = 1, limit = 10, type, startDate, endDate } = req.query;

    // Validate bank account exists
    const account = await bankAccounts.findOne({ _id: new ObjectId(id), isDeleted: { $ne: true } });
    if (!account) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }

    // Build query for transactions
    const query = {
      $or: [
        { bankAccountId: new ObjectId(id) },
        {
          "paymentDetails.bankName": account.bankName,
          "paymentDetails.accountNumber": account.accountNumber
        }
      ],
      isActive: true
    };

    // Add filters
    if (type && ['credit', 'debit'].includes(type)) {
      query.transactionType = type;
    }

    if (startDate || endDate) {
      query.date = {};
      if (startDate) query.date.$gte = new Date(startDate);
      if (endDate) query.date.$lte = new Date(endDate);
    }

    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);
    const totalCount = await transactions.countDocuments(query);

    // Get transactions
    const data = await transactions
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .toArray();

    res.json({
      success: true,
      data: {
        transactions: data,
        pagination: {
          currentPage: parseInt(page),
          totalPages: Math.ceil(totalCount / parseInt(limit)),
          totalCount,
          hasNext: skip + data.length < totalCount,
          hasPrev: parseInt(page) > 1
        }
      }
    });
  } catch (error) {
    console.error("❌ Error getting bank account transactions:", error);
    res.status(500).json({ success: false, error: "Failed to get bank account transactions" });
  }
});

// // Create bank account transaction (debit/credit)
app.post("/bank-accounts/:id/transactions", async (req, res) => {
  try {
    const { id } = req.params;
    const {
      transactionType,
      amount,
      description,
      reference,
      createdBy,
      branchId,
      notes,
      partyType,
      partyId
    } = req.body;

    // Validate required fields
    if (!transactionType || !amount || !description || !branchId) {
      return res.status(400).json({
        success: false,
        error: "Transaction type, amount, description, and branch ID are required"
      });
    }

    // Validate transaction type
    if (!['credit', 'debit'].includes(transactionType)) {
      return res.status(400).json({
        success: false,
        error: "Transaction type must be 'credit' or 'debit'"
      });
    }

    // Validate amount
    const numericAmount = Number(amount);
    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        error: "Amount must be a positive number"
      });
    }

    // Validate bank account exists
    const account = await bankAccounts.findOne({ _id: new ObjectId(id), isDeleted: { $ne: true } });
    if (!account) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }

    // Check for sufficient balance for debit transactions
    if (transactionType === 'debit' && account.currentBalance < numericAmount) {
      return res.status(400).json({
        success: false,
        error: "Insufficient balance"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).json({ success: false, error: "Invalid branch ID" });
    }

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Calculate new balance
    let newBalance = account.currentBalance;
    if (transactionType === 'credit') {
      newBalance += numericAmount;
    } else {
      newBalance -= numericAmount;
    }

    // Create transaction record
    const transactionRecord = {
      transactionId,
      transactionType,
      customerId: "BANK_ACCOUNT",
      customerName: account.accountHolder,
      customerPhone: account.contactNumber,
      customerEmail: null,
      category: "Bank Transaction",
      paymentMethod: "bank-transfer",
      paymentDetails: {
        bankName: account.bankName,
        accountNumber: account.accountNumber,
        amount: numericAmount,
        reference: reference || transactionId
      },
      customerBankAccount: {
        bankName: account.bankName,
        accountNumber: account.accountNumber
      },
      notes: notes || description,
      date: new Date(),
      createdBy: createdBy || "SYSTEM",
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      status: 'completed',
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      bankAccountId: account._id,
      isBankTransaction: true,
      description
    };

    // Update bank account balance and create transaction
    const update = {
      currentBalance: newBalance,
      updatedAt: new Date()
    };

    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id) },
      {
        $set: update,
        $push: {
          balanceHistory: {
            amount: numericAmount,
            type: transactionType === 'credit' ? 'deposit' : 'withdrawal',
            note: description,
            at: new Date(),
            transactionId
          }
        }
      },
      { returnDocument: "after" }
    );

    // Insert transaction record
    const transactionResult = await transactions.insertOne(transactionRecord);

    // If this bank transaction is tied to a party (e.g., haji/customer) and is a credit, update their paidAmount/due
    if (transactionType === 'credit' && (partyType && partyId)) {
      try {
        if (partyType === 'haji') {
          const cond = ObjectId.isValid(partyId)
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const doc = await haji.findOne(cond);
          if (doc && doc._id) {
            await haji.updateOne({ _id: doc._id }, { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } });
            const after = await haji.findOne({ _id: doc._id });
            const clamp = {};
            if ((after.paidAmount || 0) < 0) clamp.paidAmount = 0;
            if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
              clamp.paidAmount = after.totalAmount;
            }
            if (Object.keys(clamp).length) {
              clamp.updatedAt = new Date();
              await haji.updateOne({ _id: doc._id }, { $set: clamp });
            }

            // Mirror into linked customer if available on haji doc
            try {
              const linkedCustomerId = doc.customerId || doc.customer_id;
              if (linkedCustomerId) {
                const cCond = ObjectId.isValid(linkedCustomerId)
                  ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: true }
                  : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: true };
                const cDoc = await customers.findOne(cCond);
                if (cDoc && cDoc._id) {
                  const categoryText = String(serviceCategory || '').toLowerCase();
                  const isHajjCategory = categoryText.includes('haj');
                  const isUmrahCategory = categoryText.includes('umrah');
                  const update = { $set: { updatedAt: new Date() }, $inc: { paidAmount: numericAmount } };
                  // Bank deposits are credits: reduce due fields accordingly
                  const dueDelta = -numericAmount;
                  update.$inc.totalDue = (update.$inc.totalDue || 0) + dueDelta;
                  if (isHajjCategory) update.$inc.hajjDue = (update.$inc.hajjDue || 0) + dueDelta;
                  if (isUmrahCategory) update.$inc.umrahDue = (update.$inc.umrahDue || 0) + dueDelta;
                  await customers.updateOne({ _id: cDoc._id }, update);
                  const afterC = await customers.findOne({ _id: cDoc._id });
                  const clampC = {};
                  if ((afterC.totalDue || 0) < 0) clampC.totalDue = 0;
                  if ((afterC.paidAmount || 0) < 0) clampC.paidAmount = 0;
                  if ((afterC.hajjDue !== undefined) && afterC.hajjDue < 0) clampC.hajjDue = 0;
                  if ((afterC.umrahDue !== undefined) && afterC.umrahDue < 0) clampC.umrahDue = 0;
                  if (typeof afterC.totalAmount === 'number' && typeof afterC.paidAmount === 'number' && afterC.paidAmount > afterC.totalAmount) {
                    clampC.paidAmount = afterC.totalAmount;
                  }
                  if (Object.keys(clampC).length) {
                    clampC.updatedAt = new Date();
                    await customers.updateOne({ _id: cDoc._id }, { $set: clampC });
                  }
                }
              }
            } catch (mirrorErr) {
              console.warn('Bank txn: mirror haji->customer failed:', mirrorErr?.message);
            }
          }
        } else if (partyType === 'umrah') {
          // Don't filter by isActive to allow updating deleted/inactive profiles
          const cond = ObjectId.isValid(partyId)
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }] }
            : { $or: [{ customerId: partyId }, { _id: partyId }] };
          const doc = await umrah.findOne(cond);
          if (doc && doc._id) {
            await umrah.updateOne({ _id: doc._id }, { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } });
            const after = await umrah.findOne({ _id: doc._id });
            const clamp = {};
            if ((after.paidAmount || 0) < 0) clamp.paidAmount = 0;
            if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
              clamp.paidAmount = after.totalAmount;
            }
            if (Object.keys(clamp).length) {
              clamp.updatedAt = new Date();
              await umrah.updateOne({ _id: doc._id }, { $set: clamp });
            }

            // Mirror into linked customer if available on umrah doc
            try {
              const linkedCustomerId = doc.customerId || doc.customer_id;
              if (linkedCustomerId) {
                const cCond = ObjectId.isValid(linkedCustomerId)
                  ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: true }
                  : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: true };
                const cDoc = await customers.findOne(cCond);
                if (cDoc && cDoc._id) {
                  const categoryText = String(serviceCategory || '').toLowerCase();
                  const isUmrahCategory = categoryText.includes('umrah');
                  const update = { $set: { updatedAt: new Date() }, $inc: { paidAmount: numericAmount } };
                  const dueDelta = -numericAmount;
                  update.$inc.totalDue = (update.$inc.totalDue || 0) + dueDelta;
                  if (isUmrahCategory) update.$inc.umrahDue = (update.$inc.umrahDue || 0) + dueDelta;
                  await customers.updateOne({ _id: cDoc._id }, update);
                  const afterC = await customers.findOne({ _id: cDoc._id });
                  const clampC = {};
                  if ((afterC.totalDue || 0) < 0) clampC.totalDue = 0;
                  if ((afterC.paidAmount || 0) < 0) clampC.paidAmount = 0;
                  if ((afterC.umrahDue !== undefined) && afterC.umrahDue < 0) clampC.umrahDue = 0;
                  if (typeof afterC.totalAmount === 'number' && typeof afterC.paidAmount === 'number' && afterC.paidAmount > afterC.totalAmount) {
                    clampC.paidAmount = afterC.totalAmount;
                  }
                  if (Object.keys(clampC).length) {
                    clampC.updatedAt = new Date();
                    await customers.updateOne({ _id: cDoc._id }, { $set: clampC });
                  }
                }
              }
            } catch (mirrorErr) {
              console.warn('Bank txn: mirror umrah->customer failed:', mirrorErr?.message);
            }
          }
        } else if (partyType === 'customer') {
          const cond = ObjectId.isValid(partyId)
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: true }
            : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: true };
          const doc = await customers.findOne(cond);
          if (doc && doc._id) {
            await customers.updateOne({ _id: doc._id }, { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } });
            const after = await customers.findOne({ _id: doc._id });
            const clamp = {};
            if ((after.paidAmount || 0) < 0) clamp.paidAmount = 0;
            if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
              clamp.paidAmount = after.totalAmount;
            }
            if (Object.keys(clamp).length) {
              clamp.updatedAt = new Date();
              await customers.updateOne({ _id: doc._id }, { $set: clamp });
            }
            // Also mirror into haji/umrah if exists by customerId
            const hajiCond = ObjectId.isValid(partyId)
              ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
              : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
            const hDoc = await haji.findOne(hajiCond);
            if (hDoc && hDoc._id) {
              await haji.updateOne({ _id: hDoc._id }, { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } });
              const afterH = await haji.findOne({ _id: hDoc._id });
              const clampH = {};
              if ((afterH.paidAmount || 0) < 0) clampH.paidAmount = 0;
              if (typeof afterH.totalAmount === 'number' && typeof afterH.paidAmount === 'number' && afterH.paidAmount > afterH.totalAmount) {
                clampH.paidAmount = afterH.totalAmount;
              }
              if (Object.keys(clampH).length) {
                clampH.updatedAt = new Date();
                await haji.updateOne({ _id: hDoc._id }, { $set: clampH });
              }
            }
            const uCond = ObjectId.isValid(partyId)
              ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
              : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
            const uDoc = await umrah.findOne(uCond);
            if (uDoc && uDoc._id) {
              await umrah.updateOne({ _id: uDoc._id }, { $inc: { paidAmount: numericAmount }, $set: { updatedAt: new Date() } });
              const afterU = await umrah.findOne({ _id: uDoc._id });
              const clampU = {};
              if ((afterU.paidAmount || 0) < 0) clampU.paidAmount = 0;
              if (typeof afterU.totalAmount === 'number' && typeof afterU.paidAmount === 'number' && afterU.paidAmount > afterU.totalAmount) {
                clampU.paidAmount = afterU.totalAmount;
              }
              if (Object.keys(clampU).length) {
                clampU.updatedAt = new Date();
                await umrah.updateOne({ _id: uDoc._id }, { $set: clampU });
              }
            }
          }
        }
      } catch (e) {
        console.warn('Optional party paidAmount update failed:', e?.message);
      }
    }

    res.json({
      success: true,
      data: {
        transaction: {
          _id: transactionResult.insertedId,
          ...transactionRecord
        },
        bankAccount: result.value
      }
    });
  } catch (error) {
    console.error("❌ Error creating bank account transaction:", error);
    res.status(500).json({ success: false, error: "Failed to create bank account transaction" });
  }
});

// Bank account to bank account transfer
app.post("/bank-accounts/transfers", async (req, res) => {
  try {
    const {
      fromAccountId,
      toAccountId,
      amount,
      reference,
      notes,
      createdBy,
      branchId,
      accountManager
    } = req.body;

    // Validate required fields
    if (!fromAccountId || !toAccountId || !amount || !branchId) {
      return res.status(400).json({
        success: false,
        error: "From account ID, to account ID, amount, and branch ID are required"
      });
    }

    // Validate amount
    const numericAmount = Number(amount);
    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        success: false,
        error: "Amount must be a positive number"
      });
    }

    // Validate ObjectIds
    if (!ObjectId.isValid(fromAccountId) || !ObjectId.isValid(toAccountId)) {
      return res.status(400).json({
        success: false,
        error: "Invalid account ID format"
      });
    }

    // Check if same account
    if (fromAccountId === toAccountId) {
      return res.status(400).json({
        success: false,
        error: "Cannot transfer to the same account"
      });
    }

    // Get both accounts
    const fromAccount = await bankAccounts.findOne({
      _id: new ObjectId(fromAccountId),
      isDeleted: { $ne: true }
    });
    const toAccount = await bankAccounts.findOne({
      _id: new ObjectId(toAccountId),
      isDeleted: { $ne: true }
    });

    if (!fromAccount) {
      return res.status(404).json({
        success: false,
        error: "Source account not found"
      });
    }

    if (!toAccount) {
      return res.status(404).json({
        success: false,
        error: "Destination account not found"
      });
    }

    // Check sufficient balance
    if (fromAccount.currentBalance < numericAmount) {
      return res.status(400).json({
        success: false,
        error: "Insufficient balance in source account"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).json({
        success: false,
        error: "Invalid branch ID"
      });
    }

    // Generate transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Calculate new balances
    const fromNewBalance = fromAccount.currentBalance - numericAmount;
    const toNewBalance = toAccount.currentBalance + numericAmount;

    // Create transfer description
    const transferDescription = `Transfer from ${fromAccount.bankName} (${fromAccount.accountNumber}) to ${toAccount.bankName} (${toAccount.accountNumber})`;
    const transferNote = notes || `Account to Account Transfer - ${reference || transactionId}`;

    // Create master transaction record
    const masterTransaction = {
      transactionId,
      transactionType: 'transfer',
      customerId: null,
      customerName: 'Account Transfer',
      customerPhone: null,
      customerEmail: null,
      category: 'account-transfer',
      paymentMethod: 'bank-transfer',
      paymentDetails: {
        bankName: null,
        accountNumber: null,
        amount: numericAmount,
        reference: reference || transactionId
      },
      customerBankAccount: {
        bankName: null,
        accountNumber: null
      },
      sourceAccount: {
        id: fromAccount._id,
        name: fromAccount.accountHolder,
        bankName: fromAccount.bankName,
        accountNumber: fromAccount.accountNumber
      },
      destinationAccount: {
        id: toAccount._id,
        name: toAccount.accountHolder,
        bankName: toAccount.bankName,
        accountNumber: toAccount.accountNumber
      },
      notes: transferNote,
      date: new Date(),
      createdBy: createdBy || 'SYSTEM',
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      status: 'completed',
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true,
      isTransfer: true,
      transferDetails: {
        fromAccountId: fromAccount._id,
        toAccountId: toAccount._id,
        transferAmount: numericAmount,
        transferReference: reference,
        accountManager: accountManager || null
      }
    };

    // Use MongoDB transaction to ensure atomicity
    const session = db.client.startSession();

    try {
      await session.withTransaction(async () => {
        // Update source account (debit)
        await bankAccounts.findOneAndUpdate(
          { _id: fromAccount._id },
          {
            $set: {
              currentBalance: fromNewBalance,
              updatedAt: new Date()
            },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'withdrawal',
                note: `Transfer to ${toAccount.bankName} - ${toAccount.accountNumber}`,
                at: new Date(),
                transactionId
              }
            }
          },
          { session }
        );

        // Update destination account (credit)
        await bankAccounts.findOneAndUpdate(
          { _id: toAccount._id },
          {
            $set: {
              currentBalance: toNewBalance,
              updatedAt: new Date()
            },
            $push: {
              balanceHistory: {
                amount: numericAmount,
                type: 'deposit',
                note: `Transfer from ${fromAccount.bankName} - ${fromAccount.accountNumber}`,
                at: new Date(),
                transactionId
              }
            }
          },
          { session }
        );

        // Create master transaction record
        await transactions.insertOne(masterTransaction, { session });
      });

      // Get updated accounts for response
      const updatedFromAccount = await bankAccounts.findOne({ _id: fromAccount._id });
      const updatedToAccount = await bankAccounts.findOne({ _id: toAccount._id });

      res.json({
        success: true,
        message: "Transfer completed successfully",
        data: {
          transaction: masterTransaction,
          fromAccount: {
            _id: updatedFromAccount._id,
            name: updatedFromAccount.accountHolder,
            bankName: updatedFromAccount.bankName,
            accountNumber: updatedFromAccount.accountNumber,
            currentBalance: updatedFromAccount.currentBalance
          },
          toAccount: {
            _id: updatedToAccount._id,
            name: updatedToAccount.accountHolder,
            bankName: updatedToAccount.bankName,
            accountNumber: updatedToAccount.accountNumber,
            currentBalance: updatedToAccount.currentBalance
          },
          transferAmount: numericAmount,
          transactionId
        }
      });

    } finally {
      await session.endSession();
    }

  } catch (error) {
    console.error("❌ Error processing bank account transfer:", error);
    res.status(500).json({
      success: false,
      error: "Failed to process transfer"
    });
  }
});



// { Office Managment }

// ==================== HR MANAGEMENT ROUTES ====================

// ✅ POST: Create new employee
app.post("/hr/employers", async (req, res) => {
  try {
    const {
      // Personal Information
      firstName,
      lastName,
      email,
      phone,
      address,
      dateOfBirth,
      gender,
      emergencyContact,
      emergencyPhone,

      // Employment Information
      employeeId,
      position,
      department,
      manager,
      joinDate,
      employmentType,
      workLocation,
      branch,

      // Salary Information
      basicSalary,
      allowances,
      benefits,
      bankAccount,
      bankName,

      // Documents
      profilePictureUrl,
      resumeUrl,
      nidCopyUrl,
      otherDocuments = [],

      status = "active"
    } = req.body;

    // Validate required fields
    if (!firstName || !lastName || !email || !phone || !employeeId || !position || !department || !branch || !joinDate || !basicSalary) {
      return res.status(400).json({
        success: false,
        error: "Missing required fields",
        message: "First name, last name, email, phone, employee ID, position, department, branch, join date, and basic salary are required"
      });
    }

    // Check if email already exists
    const existingEmployee = await hrManagement.findOne({
      email: email.toLowerCase(),
      isActive: true
    });

    if (existingEmployee) {
      return res.status(400).json({
        success: false,
        error: "Email already exists",
        message: "An employee with this email already exists"
      });
    }

    // Check if employee ID already exists
    const existingEmployeeId = await hrManagement.findOne({
      employeeId: employeeId,
      isActive: true
    });

    if (existingEmployeeId) {
      return res.status(400).json({
        success: false,
        error: "Employee ID already exists",
        message: "An employee with this ID already exists"
      });
    }

    const newEmployee = {
      // Personal Information
      firstName: firstName.trim(),
      lastName: lastName.trim(),
      name: `${firstName.trim()} ${lastName.trim()}`, // Full name for compatibility
      email: email.toLowerCase().trim(),
      phone: phone.trim(),
      address: address || "",
      dateOfBirth: dateOfBirth || null,
      gender: gender || "",
      emergencyContact: emergencyContact || "",
      emergencyPhone: emergencyPhone || "",

      // Employment Information
      employeeId: employeeId.trim(),
      position: position.trim(),
      designation: position.trim(), // For compatibility
      department: department.trim(),
      manager: manager || "",
      joinDate: joinDate,
      joiningDate: joinDate, // For compatibility
      employmentType: employmentType || "Full-time",
      workLocation: workLocation || "",
      branch: branch,
      branchId: branch, // For compatibility

      // Salary Information
      basicSalary: parseFloat(basicSalary) || 0,
      salary: parseFloat(basicSalary) || 0, // For compatibility
      allowances: parseFloat(allowances) || 0,
      benefits: benefits || "",
      bankAccount: bankAccount || "",
      bankName: bankName || "",

      // Documents
      profilePictureUrl: profilePictureUrl || "",
      resumeUrl: resumeUrl || "",
      nidCopyUrl: nidCopyUrl || "",
      otherDocuments: otherDocuments || [],

      status,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await hrManagement.insertOne(newEmployee);

    res.status(201).json({
      success: true,
      message: "Employee created successfully",
      data: {
        id: result.insertedId,
        employeeId: newEmployee.employeeId,
        firstName: newEmployee.firstName,
        lastName: newEmployee.lastName,
        name: newEmployee.name,
        email: newEmployee.email,
        position: newEmployee.position,
        department: newEmployee.department,
        branch: newEmployee.branch
      }
    });

  } catch (error) {
    console.error("❌ Create employee error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to create employee"
    });
  }
});

// ✅ GET: Get all employees with filters
app.get("/hr/employers", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 10,
      search,
      department,
      status,
      branch,
      position,
      employmentType
    } = req.query;

    // Build filter object
    const filter = { isActive: true };

    if (search) {
      filter.$or = [
        { firstName: { $regex: search, $options: 'i' } },
        { lastName: { $regex: search, $options: 'i' } },
        { name: { $regex: search, $options: 'i' } },
        { email: { $regex: search, $options: 'i' } },
        { phone: { $regex: search, $options: 'i' } },
        { position: { $regex: search, $options: 'i' } },
        { designation: { $regex: search, $options: 'i' } },
        { employeeId: { $regex: search, $options: 'i' } }
      ];
    }

    if (department) {
      filter.department = { $regex: department, $options: 'i' };
    }

    if (status) {
      filter.status = status;
    }

    if (branch) {
      filter.$or = [
        { branch: branch },
        { branchId: branch }
      ];
    }

    if (position) {
      filter.$or = [
        { position: { $regex: position, $options: 'i' } },
        { designation: { $regex: position, $options: 'i' } }
      ];
    }

    if (employmentType) {
      filter.employmentType = employmentType;
    }

    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);
    const total = await hrManagement.countDocuments(filter);

    // Get employees with pagination
    const employees = await hrManagement
      .find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .toArray();

    res.json({
      success: true,
      data: employees,
      pagination: {
        currentPage: parseInt(page),
        totalPages: Math.ceil(total / parseInt(limit)),
        totalItems: total,
        itemsPerPage: parseInt(limit)
      }
    });

  } catch (error) {
    console.error("❌ Get employees error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to fetch employees"
    });
  }
});

// ✅ GET: Get single employee by ID
app.get("/hr/employers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const orFilters = [
      { employeeId: id },
      { employerId: id } // For backward compatibility
    ];
    if (ObjectId.isValid(id)) {
      orFilters.unshift({ _id: new ObjectId(id) });
    }

    const employee = await hrManagement.findOne({
      $or: orFilters,
      isActive: true
    });

    if (!employee) {
      return res.status(404).json({
        success: false,
        error: "Employee not found",
        message: "No employee found with the provided ID"
      });
    }

    res.json({
      success: true,
      data: employee
    });

  } catch (error) {
    console.error("❌ Get employee error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to fetch employee"
    });
  }
});

// ✅ PUT: Update employee
app.put("/hr/employers/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Remove fields that shouldn't be updated directly
    delete updateData._id;
    delete updateData.employeeId;
    delete updateData.employerId;
    delete updateData.createdAt;

    // Check if employee exists
    const existingEmployee = await hrManagement.findOne({
      $or: [
        { _id: new ObjectId(id) },
        { employeeId: id },
        { employerId: id } // For backward compatibility
      ],
      isActive: true
    });

    if (!existingEmployee) {
      return res.status(404).json({
        success: false,
        error: "Employee not found",
        message: "No employee found with the provided ID"
      });
    }

    // Check if email is being updated and if it already exists
    if (updateData.email && updateData.email !== existingEmployee.email) {
      const emailExists = await hrManagement.findOne({
        email: updateData.email.toLowerCase(),
        isActive: true,
        _id: { $ne: existingEmployee._id }
      });

      if (emailExists) {
        return res.status(400).json({
          success: false,
          error: "Email already exists",
          message: "An employee with this email already exists"
        });
      }
    }

    // Prepare update data
    const updateFields = {
      ...updateData,
      updatedAt: new Date()
    };

    // Clean up the data
    if (updateFields.firstName) {
      updateFields.firstName = updateFields.firstName.trim();
    }
    if (updateFields.lastName) {
      updateFields.lastName = updateFields.lastName.trim();
    }
    if (updateFields.firstName && updateFields.lastName) {
      updateFields.name = `${updateFields.firstName} ${updateFields.lastName}`;
    }
    if (updateFields.email) {
      updateFields.email = updateFields.email.toLowerCase().trim();
    }
    if (updateFields.phone) {
      updateFields.phone = updateFields.phone.trim();
    }
    if (updateFields.position) {
      updateFields.position = updateFields.position.trim();
      updateFields.designation = updateFields.position; // For compatibility
    }
    if (updateFields.department) {
      updateFields.department = updateFields.department.trim();
    }
    if (updateFields.basicSalary) {
      updateFields.basicSalary = parseFloat(updateFields.basicSalary) || 0;
      updateFields.salary = updateFields.basicSalary; // For compatibility
    }
    if (updateFields.allowances) {
      updateFields.allowances = parseFloat(updateFields.allowances) || 0;
    }

    const result = await hrManagement.updateOne(
      {
        $or: [
          { _id: new ObjectId(id) },
          { employeeId: id },
          { employerId: id } // For backward compatibility
        ],
        isActive: true
      },
      { $set: updateFields }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: "Employee not found",
        message: "No employee found with the provided ID"
      });
    }

    // Get updated employee
    const updatedEmployee = await hrManagement.findOne({
      $or: [
        { _id: new ObjectId(id) },
        { employeeId: id },
        { employerId: id } // For backward compatibility
      ],
      isActive: true
    });

    res.json({
      success: true,
      message: "Employee updated successfully",
      data: updatedEmployee
    });

  } catch (error) {
    console.error("❌ Update employee error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to update employee"
    });
  }
});

// ✅ DELETE: Delete employee (soft delete)
app.delete("/hr/employers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Check if employee exists
    const existingEmployee = await hrManagement.findOne({
      $or: [
        { _id: new ObjectId(id) },
        { employeeId: id },
        { employerId: id } // For backward compatibility
      ],
      isActive: true
    });

    if (!existingEmployee) {
      return res.status(404).json({
        success: false,
        error: "Employee not found",
        message: "No employee found with the provided ID"
      });
    }

    // Soft delete
    const result = await hrManagement.updateOne(
      {
        $or: [
          { _id: new ObjectId(id) },
          { employeeId: id },
          { employerId: id } // For backward compatibility
        ],
        isActive: true
      },
      {
        $set: {
          isActive: false,
          deletedAt: new Date(),
          updatedAt: new Date()
        }
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: "Employee not found",
        message: "No employee found with the provided ID"
      });
    }

    res.json({
      success: true,
      message: "Employee deleted successfully"
    });

  } catch (error) {
    console.error("❌ Delete employee error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to delete employee"
    });
  }
});

// ✅ GET: Get employee statistics
app.get("/hr/employers/stats/overview", async (req, res) => {
  try {
    const { branch, branchId } = req.query;

    const filter = { isActive: true };
    if (branch) {
      filter.$or = [
        { branch: branch },
        { branchId: branch }
      ];
    } else if (branchId) {
      filter.$or = [
        { branch: branchId },
        { branchId: branchId }
      ];
    }

    const totalEmployees = await hrManagement.countDocuments(filter);
    const activeEmployees = await hrManagement.countDocuments({ ...filter, status: "active" });
    const inactiveEmployees = await hrManagement.countDocuments({ ...filter, status: "inactive" });

    // Get department-wise count
    const departmentStats = await hrManagement.aggregate([
      { $match: filter },
      { $group: { _id: "$department", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();

    // Get position-wise count
    const positionStats = await hrManagement.aggregate([
      { $match: filter },
      { $group: { _id: "$position", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();

    // Get employment type-wise count
    const employmentTypeStats = await hrManagement.aggregate([
      { $match: filter },
      { $group: { _id: "$employmentType", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray();

    res.json({
      success: true,
      data: {
        totalEmployees,
        activeEmployees,
        inactiveEmployees,
        departmentStats,
        positionStats,
        employmentTypeStats
      }
    });

  } catch (error) {
    console.error("❌ Get employee stats error:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error",
      message: "Failed to fetch employee statistics"
    });
  }
});


// Start server only if not in Vercel environment
if (process.env.NODE_ENV !== 'production' || !process.env.VERCEL) {
  const host = process.env.HOST || '0.0.0.0';
  const basePort = Number(process.env.PORT) || 3000;
  const maxRetries = Number(process.env.PORT_MAX_RETRY || 10);

  const listenWithRetry = (tryPort, attempt = 0) => {
    const server = http.createServer(app);

    server.on('listening', () => {
      const address = server.address();
      const actualPort = address && address.port ? address.port : tryPort;
      if (!process.env.PORT) {
        // Reflect chosen port so logs/tools know it
        process.env.PORT = String(actualPort);
      }
      console.log(`🚀 Server is running on http://${host}:${actualPort}`);
    });

    server.on('error', (err) => {
      if (err && err.code === 'EADDRINUSE' && attempt < maxRetries) {
        const nextPort = tryPort + 1;
        console.warn(`⚠️  Port ${tryPort} in use. Retrying on ${nextPort} (attempt ${attempt + 1}/${maxRetries})...`);
        // Try next port
        setTimeout(() => listenWithRetry(nextPort, attempt + 1), 200);
      } else {
        console.error('❌ Server failed to start:', err);
      }
    });

    server.listen(tryPort, host);
  };

  listenWithRetry(basePort, 0);
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  await client.close();
  process.exit(0);
});



// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    success: false,
    error: 'Internal server error',
    message: 'Something went wrong'
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    success: false,
    error: 'Not Found',
    message: 'API endpoint not found'
  });
});

// Export the app for Vercel
module.exports = app;
