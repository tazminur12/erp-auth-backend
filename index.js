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
    'https://erp-dashboard-umber.vercel.app' // Netlify production
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
        // First try airCustomers collection
        const isValidObjectId = ObjectId.isValid(tx.partyId);
        const airCustomerCondition = isValidObjectId
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: { $ne: false } }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: { $ne: false } };
        customer = await airCustomers.findOne(airCustomerCondition);
        // If not found in airCustomers, try regular customers collection
        if (!customer) {
          const cond = isValidObjectId
            ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
            : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: true };
          try {
            customer = await customers.findOne(cond);
          } catch (e) {
            // customers collection doesn't exist or error, continue
          }
        }
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
        // First try airCustomers collection (main collection for air customers)
        const isValidObjectId = ObjectId.isValid(tx.partyId);
        const airCustomerCondition = isValidObjectId
          ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: { $ne: false } }
          : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: { $ne: false } };
        let doc = await airCustomers.findOne(airCustomerCondition, { session });
        let isAirCustomer = !!doc;
        let customerCollection = airCustomers;

        // If not found in airCustomers, try regular customers collection
        if (!doc) {
          const cond = isValidObjectId
            ? { $or: [{ customerId: tx.partyId }, { _id: new ObjectId(tx.partyId) }], isActive: true }
            : { $or: [{ customerId: tx.partyId }, { _id: tx.partyId }], isActive: true };
          try {
            doc = await customers.findOne(cond, { session });
            if (doc) {
              customerCollection = customers;
            }
          } catch (e) {
            // customers collection doesn't exist or error, continue with airCustomers only
          }
        }

        if (doc) {
          const incObj = { totalDue: dueDelta };
          // Optional: when customer pays us (credit), track totalPaid
          if (hasValidAmount && transactionType === 'credit') {
            incObj.paidAmount = (incObj.paidAmount || 0) + numericAmount;
          }
          if (isHajjCategory) {
            // customers often store hajjDue
            incObj.hajjDue = (incObj.hajjDue || 0) + dueDelta;
          }
          if (isUmrahCategory) {
            incObj.umrahDue = (incObj.umrahDue || 0) + dueDelta;
          }
          // For airCustomers, also update totalAmount on debit (when customer owes more)
          if (isAirCustomer && transactionType === 'debit') {
            incObj.totalAmount = (incObj.totalAmount || 0) + numericAmount;
          }

          await customerCollection.updateOne(
            { _id: doc._id },
            { $inc: incObj, $set: { updatedAt: new Date() } },
            { session }
          );

          // Clamp negatives
          const after = await customerCollection.findOne({ _id: doc._id }, { session });
          const setClamp = {};
          if ((after.totalDue || 0) < 0) setClamp['totalDue'] = 0;
          if ((after.paidAmount || 0) < 0) setClamp['paidAmount'] = 0;
          if (typeof after.hajjDue !== 'undefined' && after.hajjDue < 0) setClamp['hajjDue'] = 0;
          if (typeof after.umrahDue !== 'undefined' && after.umrahDue < 0) setClamp['umrahDue'] = 0;
          // For airCustomers, ensure paidAmount doesn't exceed totalAmount
          if (isAirCustomer && typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
            setClamp['paidAmount'] = after.totalAmount;
          }
          if (Object.keys(setClamp).length) {
            setClamp.updatedAt = new Date();
            await customerCollection.updateOne({ _id: doc._id }, { $set: setClamp }, { session });
          }
          updatedCustomer = await customerCollection.findOne({ _id: doc._id }, { session });

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
              await triggerFamilyRecomputeForHaji(afterH, { session });
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
          await triggerFamilyRecomputeForHaji(after, { session });
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
          await triggerFamilyRecomputeForUmrah(after, { session });
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
const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASSWORD}@salma-air-erp.puabxco.mongodb.net/?appName=Salma-Air-ERP`;
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

// Helper: safely create ObjectId
const toObjectId = (value) => {
  if (value === null || value === undefined) return null;
  return ObjectId.isValid(value) ? new ObjectId(value) : null;
};

// Helper: recompute family totals for a primary haji profile
async function recomputeFamilyTotals(primaryId, { session } = {}) {
  const primaryObjectId = toObjectId(primaryId);
  if (!primaryObjectId) return null;

  const primaryDoc = await haji.findOne({ _id: primaryObjectId }, { session });
  if (!primaryDoc) return null;

  const dependents = await haji
    .find({ primaryHolderId: primaryObjectId }, { session })
    .toArray();

  const allMembers = [primaryDoc, ...dependents];
  const familyTotal = allMembers.reduce(
    (sum, member) => sum + Number(member?.totalAmount || 0),
    0
  );
  const familyPaid = allMembers.reduce(
    (sum, member) => sum + Number(member?.paidAmount || 0),
    0
  );
  const familyDue = Math.max(familyTotal - familyPaid, 0);

  await haji.updateOne(
    { _id: primaryObjectId },
    { $set: { familyTotal, familyPaid, familyDue, updatedAt: new Date() } },
    { session }
  );

  return {
    familyTotal,
    familyPaid,
    familyDue,
    members: allMembers,
  };
}

// Helper: trigger recompute using a haji document (uses primaryHolderId when present)
async function triggerFamilyRecomputeForHaji(hajiDoc, { session } = {}) {
  if (!hajiDoc) return;
  const target = hajiDoc.primaryHolderId || hajiDoc._id;
  if (!target) return;
  await recomputeFamilyTotals(target, { session });
}

// Helper: recompute family totals for a primary umrah profile
async function recomputeUmrahFamilyTotals(primaryId, { session } = {}) {
  const primaryObjectId = toObjectId(primaryId);
  if (!primaryObjectId) return null;

  const primaryDoc = await umrah.findOne({ _id: primaryObjectId }, { session });
  if (!primaryDoc) return null;

  const dependents = await umrah
    .find({ primaryHolderId: primaryObjectId }, { session })
    .toArray();

  const allMembers = [primaryDoc, ...dependents];
  const familyTotal = allMembers.reduce(
    (sum, member) => sum + Number(member?.totalAmount || 0),
    0
  );
  const familyPaid = allMembers.reduce(
    (sum, member) => sum + Number(member?.paidAmount || 0),
    0
  );
  const familyDue = Math.max(familyTotal - familyPaid, 0);

  await umrah.updateOne(
    { _id: primaryObjectId },
    { $set: { familyTotal, familyPaid, familyDue, updatedAt: new Date() } },
    { session }
  );

  return {
    familyTotal,
    familyPaid,
    familyDue,
    members: allMembers,
  };
}

// Helper: trigger recompute using an umrah document
async function triggerFamilyRecomputeForUmrah(umrahDoc, { session } = {}) {
  if (!umrahDoc) return;
  const target = umrahDoc.primaryHolderId || umrahDoc._id;
  if (!target) return;
  await recomputeUmrahFamilyTotals(target, { session });
}

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

// Helper: Generate unique Customer ID based on customer type prefix
const generateCustomerId = async (db, customerType) => {
  const counterCollection = db.collection("counters");
  
  // Get customer type to find prefix
  let prefix = "AIR"; // Default prefix
  if (customerType) {
    const customerTypeDoc = await customerTypes.findOne({ 
      value: customerType.toLowerCase(),
      isActive: true 
    });
    if (customerTypeDoc && customerTypeDoc.prefix) {
      prefix = customerTypeDoc.prefix;
    }
  }
  
  // Create counter key for customer type
  const counterKey = `customer_${prefix}`;
  
  // Special handling for 'haj' type: reset counter if no records exist
  if (customerType && customerType.toLowerCase() === 'haj') {
    const hajiCollection = db.collection("haji");
    const hajiCount = await hajiCollection.countDocuments({});
    
    // If no haji records exist, reset counter to 0
    if (hajiCount === 0) {
      const existingCounter = await counterCollection.findOne({ counterKey });
      if (existingCounter && existingCounter.sequence > 0) {
        await counterCollection.updateOne(
          { counterKey },
          { $set: { sequence: 0 } }
        );
      }
    }
  }
  
  // Special handling for 'umrah' type: ensure sequential numbering from 1
  if (customerType && customerType.toLowerCase() === 'umrah') {
    const umrahCollection = db.collection("umrah");
    const umrahCount = await umrahCollection.countDocuments({});
    
    if (umrahCount === 0) {
      // If no umrah records exist, reset counter to 0 (so first will be 1)
      const existingCounter = await counterCollection.findOne({ counterKey });
      if (existingCounter && existingCounter.sequence > 0) {
        await counterCollection.updateOne(
          { counterKey },
          { $set: { sequence: 0 } }
        );
      }
    } else {
      // If umrah records exist, sync counter with max customerId from actual data
      const maxUmrah = await umrahCollection
        .find({ customerId: { $exists: true, $ne: null } })
        .sort({ customerId: -1 })
        .limit(1)
        .toArray();
      
      if (maxUmrah.length > 0 && maxUmrah[0].customerId) {
        // Extract number from customerId (e.g., "UMRAH-0042" -> 42)
        const customerIdStr = String(maxUmrah[0].customerId);
        const match = customerIdStr.match(/-(\d+)$/);
        if (match) {
          const maxNumber = parseInt(match[1], 10);
          const existingCounter = await counterCollection.findOne({ counterKey });
          
          // Update counter if it's lower than the max found
          if (!existingCounter || existingCounter.sequence < maxNumber) {
            await counterCollection.updateOne(
              { counterKey },
              { $set: { sequence: maxNumber } },
              { upsert: true }
            );
          }
        }
      }
    }
  }
  
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
  
  // Format: AIR-0001, HAJI-0001, etc.
  return `${prefix}-${String(newSequence).padStart(4, '0')}`;
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

// Helper: Generate unique Loan ID (per direction per day)
const generateLoanId = async (db, loanDirection) => {
  const counterCollection = db.collection("counters");

  // Get current date in DDMMYY format
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);

  const dir = (loanDirection || '').toLowerCase() === 'giving' ? 'LG' : 'LR';
  const counterKey = `loan_${dir}_${dateStr}`;

  let counter = await counterCollection.findOne({ counterKey });
  if (!counter) {
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }

  const newSequence = counter.sequence + 1;
  await counterCollection.updateOne(
    { counterKey },
    { $set: { sequence: newSequence } }
  );

  const serial = String(newSequence).padStart(4, '0');
  return `${dir}${dateStr}${serial}`; // e.g., LG2508290001 or LR2508290001
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
let db, users, branches, counters, customerTypes, airCustomers, otherCustomers, passportServices, manpowerServices, visaProcessingServices, ticketChecks, oldTicketReissues, otherServices, services, vendors, orders, bankAccounts, categories, operatingExpenseCategories, personalExpenseCategories, personalExpenseTransactions, agents, hrManagement, haji, umrah, agentPackages, packages, transactions, invoices, accounts, vendorBills, loans, cattle, milkProductions, feedTypes, feedStocks, feedUsages, healthRecords, vaccinations, vetVisits, breedings, calvings, farmEmployees, attendanceRecords, farmExpenses, farmIncomes, exchanges, airlines, tickets, notifications, licenses, vendorBankAccounts;

// Initialize database connection
async function initializeDatabase() {
  try {
    await client.connect();
    console.log("✅ MongoDB connected");

    db = client.db("erpDashboard");
    users = db.collection("users");
    notifications = db.collection("notifications");
    branches = db.collection("branches");
    counters = db.collection("counters");
    customerTypes = db.collection("customerTypes");
    airCustomers = db.collection("airCustomers");
    otherCustomers = db.collection("otherCustomers");
    passportServices = db.collection("passportServices");
    manpowerServices = db.collection("manpowerServices");
    visaProcessingServices = db.collection("visaProcessingServices");
    ticketChecks = db.collection("ticketChecks");
    oldTicketReissues = db.collection("oldTicketReissues");
    otherServices = db.collection("otherServices");
    services = db.collection("services");
    vendors = db.collection("vendors");
    orders = db.collection("orders");
    bankAccounts = db.collection("bankAccounts");
    categories = db.collection("categories");
    operatingExpenseCategories = db.collection("operatingExpenseCategories");
    personalExpenseCategories = db.collection("personalExpenseCategories");
    personalExpenseTransactions = db.collection("personalExpenseTransactions");
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
    vendorBankAccounts = db.collection("vendorBankAccounts");
    loans = db.collection("loans");
    // Miraj vai Section 
    cattle = db.collection("cattle");
    milkProductions = db.collection("milkProductions");
    feedTypes = db.collection("feedTypes");
    feedStocks = db.collection("feedStocks");
    feedUsages = db.collection("feedUsages");
    healthRecords = db.collection("healthRecords");
    vaccinations = db.collection("vaccinations");
    vetVisits = db.collection("vetVisits");
    breedings = db.collection("breedings");
    calvings = db.collection("calvings");
    // Farm HR (for EmployeeManagement frontend)
    farmEmployees = db.collection("farmEmployees");
    attendanceRecords = db.collection("attendanceRecords");
    // Farm Finance (for FinancialReport frontend)
    farmExpenses = db.collection("farmExpenses");
    farmIncomes = db.collection("farmIncomes");
    // Currency Exchange
    exchanges = db.collection("exchanges");
    // Airlines
    airlines = db.collection("airlines");
    // Air Ticketing Tickets
    tickets = db.collection("airTickets");
    // Licenses
    licenses = db.collection("licenses");
  





    // Initialize default branches
    await initializeDefaultBranches(db, branches, counters);

    // Initialize default customer types
    await initializeDefaultCustomerTypes(db, customerTypes);

    // Create useful indexes (non-blocking if already exist)
    try {
      await Promise.all([
        // Transactions (kept)
        transactions.createIndex({ loanId: 1, isActive: 1, createdAt: -1 }, { name: "tx_loan_active_createdAt" }),
        // Loans collection indexes
        loans.createIndex({ loanId: 1 }, { unique: true, name: "loans_loanId_unique" }),
        loans.createIndex({ loanDirection: 1, status: 1, createdAt: -1 }, { name: "loans_direction_status_createdAt" }),
        // Personal expense categories: unique name (case-insensitive)
        personalExpenseCategories.createIndex(
          { name: 1 },
          { unique: true, name: "personalExpenseCategories_name_unique", collation: { locale: "en", strength: 2 } }
        ),
        // Personal expense transactions helpful indexes (debit only)
        personalExpenseTransactions.createIndex({ date: 1 }, { name: "pet_date" }),
        personalExpenseTransactions.createIndex({ categoryId: 1, date: -1 }, { name: "pet_category_date" }),
        personalExpenseTransactions.createIndex({ createdAt: -1 }, { name: "pet_createdAt_desc" })
        ,
        // Cattle helpful indexes
        cattle.createIndex({ createdAt: -1 }, { name: "cattle_createdAt_desc" }),
        cattle.createIndex({ tagNumber: 1 }, { sparse: true, name: "cattle_tagNumber" }),
        // Milk production indexes
        milkProductions.createIndex({ cattleId: 1, date: -1 }, { name: "milk_cattleId_date_desc" }),
        milkProductions.createIndex({ date: -1 }, { name: "milk_date_desc" }),
        // Feed management indexes
        feedTypes.createIndex({ name: 1 }, { unique: true, name: "feedTypes_name_unique", collation: { locale: "en", strength: 2 } }),
        feedStocks.createIndex({ feedTypeId: 1, purchaseDate: -1 }, { name: "feedStocks_type_purchaseDate_desc" }),
        feedStocks.createIndex({ expiryDate: 1 }, { name: "feedStocks_expiry" }),
        feedUsages.createIndex({ feedTypeId: 1, date: -1 }, { name: "feedUsages_type_date_desc" }),
        feedUsages.createIndex({ date: -1 }, { name: "feedUsages_date_desc" }),
        // Health/Vaccinations/VetVisits indexes
        healthRecords.createIndex({ cattleId: 1, date: -1 }, { name: "health_cattleId_date_desc" }),
        vaccinations.createIndex({ cattleId: 1, date: -1 }, { name: "vacc_cattleId_date_desc" }),
        vaccinations.createIndex({ nextDueDate: 1 }, { name: "vacc_nextDueDate" }),
        vetVisits.createIndex({ cattleId: 1, date: -1 }, { name: "visit_cattleId_date_desc" })
        ,
        // Breeding & Calving indexes
        breedings.createIndex({ cowId: 1, breedingDate: -1 }, { name: "breed_cow_breedingDate_desc" }),
        breedings.createIndex({ expectedCalvingDate: 1 }, { name: "breed_expectedCalvingDate" }),
        calvings.createIndex({ cowId: 1, calvingDate: -1 }, { name: "calv_cow_calvingDate_desc" })
        ,
        // Farm HR helpful indexes
        farmEmployees.createIndex({ id: 1 }, { unique: true, name: "farmEmployees_id_unique" }),
        farmEmployees.createIndex({ status: 1, createdAt: -1 }, { name: "farmEmployees_status_createdAt" }),
        farmEmployees.createIndex({ name: 1 }, { name: "farmEmployees_name" }),
        farmEmployees.createIndex({ phone: 1 }, { name: "farmEmployees_phone" }),
        attendanceRecords.createIndex({ employeeId: 1, date: -1 }, { name: "attendance_employee_date_desc" }),
        attendanceRecords.createIndex({ date: -1, status: 1 }, { name: "attendance_date_status" })
        ,
        // Farm finance indexes
        farmExpenses.createIndex({ id: 1 }, { unique: true, name: "farmExpenses_id_unique" }),
        farmExpenses.createIndex({ createdAt: -1 }, { name: "farmExpenses_createdAt_desc" }),
        farmExpenses.createIndex({ category: 1, createdAt: -1 }, { name: "farmExpenses_category_createdAt" }),
        farmIncomes.createIndex({ id: 1 }, { unique: true, name: "farmIncomes_id_unique" }),
        farmIncomes.createIndex({ date: -1 }, { name: "farmIncomes_date_desc" }),
        farmIncomes.createIndex({ source: 1, date: -1 }, { name: "farmIncomes_source_date" }),
        // Air Customers indexes
        airCustomers.createIndex({ customerId: 1 }, { unique: true, name: "airCustomers_customerId_unique" }),
        // Air Tickets indexes
        tickets.createIndex({ ticketId: 1 }, { unique: true, name: "airTickets_ticketId_unique" }),
        tickets.createIndex({ bookingId: 1 }, { name: "airTickets_bookingId" }),
        airCustomers.createIndex({ mobile: 1 }, { name: "airCustomers_mobile" }),
        airCustomers.createIndex({ email: 1 }, { sparse: true, name: "airCustomers_email" }),
        airCustomers.createIndex({ passportNumber: 1 }, { sparse: true, name: "airCustomers_passportNumber" }),
        airCustomers.createIndex({ isActive: 1, createdAt: -1 }, { name: "airCustomers_active_createdAt" }),
        airCustomers.createIndex({ name: "text", mobile: "text", email: "text", passportNumber: "text" }, { name: "airCustomers_text_search" }),
        // Notifications
        notifications.createIndex({ userId: 1, isRead: 1, isActive: 1, createdAt: -1 }, { name: "notifications_user_read_active_createdAt" }),
        notifications.createIndex({ isActive: 1, createdAt: -1 }, { name: "notifications_active_createdAt" }),
        // HR Management
        hrManagement.createIndex({
          name: "text",
          email: "text",
          phone: "text",
          employeeId: "text",
          position: "text",
          firstName: "text",
          lastName: "text"
        }, {
          name: "hr_search_index",
          background: true
        })
      ]);
    } catch (e) {
      console.warn("⚠️ Index creation warning:", e.message);
    }
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

// ==================== OTP LOGIN SYSTEM ====================

// In-memory OTP store (phone → { otp, expiresAt })
const otpStore = new Map();

// Utility: Generate 6-digit OTP
function generateOTP() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

// Utility: Send SMS via sms.net.bd
async function sendSMS(phone, message) {
  try {
    const apiKey = process.env.SMS_API_KEY;
    const senderId = process.env.SMS_SENDER_ID;

    // Debug log to check what sender ID is being used
    console.log('📱 SMS Configuration:', {
      senderIdConfigured: !!senderId,
      senderIdValue: senderId,
      apiKeyConfigured: !!apiKey
    });

    if (!apiKey || !senderId) {
      throw new Error('SMS credentials not configured');
    }

    // Normalize phone number (ensure it starts with country code)
    let normalizedPhone = phone.replace(/\D/g, ''); // Remove non-digits
    if (normalizedPhone.startsWith('0')) {
      normalizedPhone = '88' + normalizedPhone; // Add Bangladesh country code
    } else if (!normalizedPhone.startsWith('88')) {
      normalizedPhone = '88' + normalizedPhone;
    }

    const payload = new URLSearchParams();
    payload.append('api_key', apiKey);
    payload.append('senderid', senderId);
    payload.append('to', normalizedPhone);
    payload.append('msg', message);

    console.log('📤 Sending SMS:', {
      to: normalizedPhone,
      from: senderId,
      messageLength: message.length
    });

    const response = await fetch('https://api.sms.net.bd/sendsms', {
      method: 'POST',
      body: payload,
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('SMS API error:', errorText);
      throw new Error(`SMS API responded with ${response.status}`);
    }

    const result = await response.text();
    console.log('SMS sent successfully:', { phone: normalizedPhone, result });
    return { success: true, result };

  } catch (error) {
    console.error('SMS sending failed:', error);
    throw error;
  }
}

// POST: Send OTP to phone number
app.post("/api/auth/send-otp", async (req, res) => {
  try {
    const { phone } = req.body;

    // Validation
    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^(\+?88)?0?1[3-9]\d{8}$/;
    const cleanPhone = phone.replace(/\s|-/g, '');
    
    if (!phoneRegex.test(cleanPhone)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format. Use Bangladesh mobile number."
      });
    }

    // Normalize phone number for storage
    let normalizedPhone = cleanPhone.replace(/\D/g, '');
    if (normalizedPhone.startsWith('0')) {
      normalizedPhone = '88' + normalizedPhone;
    } else if (!normalizedPhone.startsWith('88')) {
      normalizedPhone = '88' + normalizedPhone;
    }

    // Generate OTP
    const otp = generateOTP();
    const expiresAt = new Date(Date.now() + 5 * 60 * 1000); // 5 minutes from now

    // Store OTP with expiry
    otpStore.set(normalizedPhone, {
      otp,
      expiresAt,
      attempts: 0
    });

    // Send OTP via SMS
    const smsMessage = `[Salma Air] Your login OTP is ${otp}. Valid for 5 minutes. Do not share this code.`;
    
    try {
      await sendSMS(normalizedPhone, smsMessage);
    } catch (smsError) {
      console.error('SMS sending failed:', smsError);
      // Clean up OTP store on SMS failure
      otpStore.delete(normalizedPhone);
      
      return res.status(500).json({
        success: false,
        error: true,
        message: "Failed to send OTP. Please try again.",
        details: smsError.message
      });
    }

    console.log(`OTP sent to ${normalizedPhone}: ${otp} (expires at ${expiresAt.toISOString()})`);

    res.json({
      success: true,
      message: "OTP sent successfully to your phone number",
      phone: normalizedPhone,
      expiresIn: 300 // seconds
    });

  } catch (error) {
    console.error('Send OTP error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while sending OTP",
      details: error.message
    });
  }
});

// POST: Verify OTP and login/register user
app.post("/api/auth/verify-otp", async (req, res) => {
  try {
    const { phone, otp } = req.body;

    // Validation
    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    if (!otp || !otp.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "OTP is required"
      });
    }

    // Normalize phone number
    let normalizedPhone = phone.replace(/\D/g, '');
    if (normalizedPhone.startsWith('0')) {
      normalizedPhone = '88' + normalizedPhone;
    } else if (!normalizedPhone.startsWith('88')) {
      normalizedPhone = '88' + normalizedPhone;
    }

    // Check if OTP exists
    const otpData = otpStore.get(normalizedPhone);
    
    if (!otpData) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "OTP not found. Please request a new OTP."
      });
    }

    // Check if OTP is expired
    if (new Date() > otpData.expiresAt) {
      // Clean up expired OTP
      otpStore.delete(normalizedPhone);
      
      return res.status(400).json({
        success: false,
        error: true,
        message: "OTP has expired. Please request a new OTP."
      });
    }

    // Check if OTP matches
    if (otpData.otp !== otp.trim()) {
      // Increment failed attempts
      otpData.attempts = (otpData.attempts || 0) + 1;
      
      // Block after 3 failed attempts
      if (otpData.attempts >= 3) {
        otpStore.delete(normalizedPhone);
        return res.status(400).json({
          success: false,
          error: true,
          message: "Too many failed attempts. Please request a new OTP."
        });
      }
      
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid OTP. Please try again.",
        attemptsLeft: 3 - otpData.attempts
      });
    }

    // OTP is valid - proceed with login/registration
    
    // Check if user exists with this phone number
    let user = await users.findOne({
      phone: normalizedPhone,
      isActive: true
    });

    // If user doesn't exist, create new user
    if (!user) {
      // Get default branch or first active branch
      let branch = await branches.findOne({ branchId: 'main', isActive: true });
      if (!branch) {
        branch = await branches.findOne({ isActive: true });
      }
      
      if (!branch) {
        // Create default branch if none exists
        branch = {
          branchId: 'main',
          branchName: 'Main Branch',
          branchCode: 'MN',
          branchLocation: 'Head Office'
        };
        await branches.insertOne({
          ...branch,
          isActive: true,
          createdAt: new Date(),
          updatedAt: new Date()
        });
      }

      // Generate unique ID for the user
      const uniqueId = await generateUniqueId(db, branch.branchCode);

      // Create new user
      const newUser = {
        uniqueId,
        phone: normalizedPhone,
        displayName: `User ${normalizedPhone.slice(-4)}`, // Default name
        email: null, // No email for phone-based login
        branchId: branch.branchId,
        branchName: branch.branchName,
        branchLocation: branch.branchLocation,
        firebaseUid: null, // Not using Firebase for OTP login
        role: 'user',
        loginMethod: 'otp', // Mark as OTP login
        isActive: true,
        createdAt: new Date(),
        updatedAt: new Date()
      };

      const result = await users.insertOne(newUser);
      user = { ...newUser, _id: result.insertedId };

      console.log(`✅ New OTP user created: ${uniqueId} (${normalizedPhone})`);
    }

    // Delete OTP after successful verification
    otpStore.delete(normalizedPhone);

    // Generate JWT token using existing logic
    const token = jwt.sign(
      {
        sub: user._id.toString(),
        uniqueId: user.uniqueId,
        phone: user.phone,
        email: user.email,
        role: user.role,
        branchId: user.branchId,
        loginMethod: 'otp'
      },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRES || '7d' }
    );

    res.json({
      success: true,
      message: user._id ? "Login successful" : "User created and logged in successfully",
      token,
      user: {
        uniqueId: user.uniqueId,
        phone: user.phone,
        displayName: user.displayName,
        email: user.email,
        role: user.role,
        branchId: user.branchId,
        branchName: user.branchName,
        loginMethod: 'otp'
      }
    });

  } catch (error) {
    console.error('Verify OTP error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while verifying OTP",
      details: error.message
    });
  }
});

// Optional: Clear expired OTPs periodically (cleanup job)
setInterval(() => {
  const now = new Date();
  let cleanedCount = 0;
  
  for (const [phone, data] of otpStore.entries()) {
    if (now > data.expiresAt) {
      otpStore.delete(phone);
      cleanedCount++;
    }
  }
  
  if (cleanedCount > 0) {
    console.log(`🧹 Cleaned up ${cleanedCount} expired OTPs`);
  }
}, 60000); // Run every 1 minute

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
    const { email, displayName, branchId, firebaseUid, role = "user", phone } = req.body;

    if (!email || !displayName || !branchId || !firebaseUid) {
      return res.status(400).send({
        error: true,
        message: "Email, displayName, branchId, and firebaseUid are required"
      });
    }

    // Validate phone number format if provided
    if (phone) {
      const phoneRegex = /^(?:\+?880|0)?1[3-9]\d{8}$/;
      if (!phoneRegex.test(phone.replace(/\s+/g, ''))) {
        return res.status(400).send({
          error: true,
          message: "Invalid phone number format. Use Bangladesh mobile number (e.g., 01712345678)"
        });
      }
    }

    // Check if user already exists by email
    const exists = await users.findOne({ email: email.toLowerCase() });
    if (exists) {
      return res.status(400).send({ error: true, message: "User already exists with this email" });
    }

    // Check if phone number already exists (if provided)
    if (phone) {
      const normalizedPhone = phone.replace(/\s+/g, '').replace(/^\+?880/, '0').replace(/^88/, '0');
      const phoneExists = await users.findOne({ phone: normalizedPhone });
      if (phoneExists) {
        return res.status(400).send({ error: true, message: "User already exists with this phone number" });
      }
    }

    // Get branch information
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).send({ error: true, message: "Invalid branch ID" });
    }

    // Generate unique ID for the user
    const uniqueId = await generateUniqueId(db, branch.branchCode);

    // Normalize phone number for storage
    const normalizedPhone = phone ? phone.replace(/\s+/g, '').replace(/^\+?880/, '0').replace(/^88/, '0') : null;

    // Create new user
    const newUser = {
      uniqueId,
      displayName,
      email: email.toLowerCase(),
      phone: normalizedPhone,
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
        phone: newUser.phone,
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
      name: user.displayName || user.name, // Ensure name is available
      email: user.email,
      phone: user.phone || '',
      address: user.address || '',
      department: user.department || '',
      role: user.role,
      branchId: user.branchId,
      branchName: user.branchName,
      photoURL: user.photoURL || null,
      createdAt: user.createdAt,
      lastLoginAt: user.lastLoginAt, // Added for frontend "Last Login"
      isActive: user.isActive // Added for frontend "Status"
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
    if (filteredUpdateData.name) {
      filteredUpdateData.displayName = filteredUpdateData.name;
    }

    // Add update timestamp
    filteredUpdateData.updatedAt = new Date();

    // Validate and normalize phone number if provided
    if (filteredUpdateData.phone) {
      const phoneRegex = /^(?:\+?880|0)?1[3-9]\d{8}$/;
      if (!phoneRegex.test(filteredUpdateData.phone.replace(/\s+/g, ''))) {
        return res.status(400).json({
          error: true,
          message: "Invalid phone number format. Use Bangladesh mobile number (e.g., 01712345678)"
        });
      }

      // Normalize phone number
      const normalizedPhone = filteredUpdateData.phone.replace(/\s+/g, '').replace(/^\+?880/, '0').replace(/^88/, '0');
      
      // Check if phone number already exists for another user
      const phoneExists = await users.findOne({
        phone: normalizedPhone,
        email: { $ne: email }, // Exclude current user
        isActive: true
      });

      if (phoneExists) {
        return res.status(400).json({
          error: true,
          message: "This phone number is already registered to another user"
        });
      }

      filteredUpdateData.phone = normalizedPhone;
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
        name: updatedUser.displayName || updatedUser.name,
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
        lastLoginAt: updatedUser.lastLoginAt,
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

    // Check if any customers are using this type (check both collections)
    const [airCustomersUsingType, otherCustomersUsingType] = await Promise.all([
      airCustomers.countDocuments({
        customerType: customerTypeToDelete.value,
        isActive: { $ne: false }
      }),
      otherCustomers.countDocuments({
        customerType: customerTypeToDelete.value,
        isActive: { $ne: false }
      })
    ]);

    const totalCustomersUsingType = airCustomersUsingType + otherCustomersUsingType;

    if (totalCustomersUsingType > 0) {
      return res.status(400).json({
        error: true,
        message: `Cannot delete customer type. ${totalCustomersUsingType} customers are using this type.`
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


// ==================== AIR CUSTOMER CRUD ROUTES ====================

// POST: Create new Air Customer
app.post("/api/airCustomers", async (req, res) => {
  try {
    const {
      // Basic customer information
      firstName,
      lastName,
      mobile,
      email,
      occupation,
      address,
      division,
      district,
      upazila,
      postCode,
      whatsappNo,
      customerType,
      
      // Image data
      customerImage,
      passportCopy,
      nidCopy,
      
      // Passport information
      passportNumber,
      passportType,
      issueDate,
      expiryDate,
      dateOfBirth,
      nidNumber,
      passportFirstName,
      passportLastName,
      nationality,
      previousPassport,
      gender,
      
      // Family details
      fatherName,
      motherName,
      spouseName,
      maritalStatus,
      
      // Additional information
      notes,
      referenceBy,
      referenceCustomerId
    } = req.body;

    // Validation - firstName and mobile are required
    if (!firstName || !firstName.trim()) {
      return res.status(400).json({
        success: false,
        message: "First name is required"
      });
    }

    if (!mobile) {
      return res.status(400).json({
        success: false,
        message: "Mobile number is required"
      });
    }

    // Validate mobile number format (Bangladeshi format: 01XXXXXXXXX)
    const mobileRegex = /^01[3-9]\d{8}$/;
    if (!mobileRegex.test(mobile)) {
      return res.status(400).json({
        success: false,
        message: "Invalid mobile number format. Please use format: 01XXXXXXXXX"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email)) {
        return res.status(400).json({
          success: false,
          message: "Invalid email format"
        });
      }
    }

    // Check if mobile number already exists
    const existingCustomer = await airCustomers.findOne({
      mobile: mobile,
      isActive: { $ne: false }
    });

    if (existingCustomer) {
      return res.status(400).json({
        success: false,
        message: "Customer with this mobile number already exists"
      });
    }

    // Generate customer ID
    const customerId = await generateCustomerId(db, customerType);

    // Create customer object
    const customerData = {
      customerId,
      // Basic customer information
      name: `${firstName || ''}${lastName ? ' ' + lastName : ''}`.trim(),
      firstName: firstName || null,
      lastName: lastName || null,
      mobile,
      email: email || null,
      occupation: occupation || null,
      address: address || null,
      division: division || null,
      district: district || null,
      upazila: upazila || null,
      postCode: postCode || null,
      whatsappNo: whatsappNo || null,
      customerType: customerType || null,
      
     passportCopy: passportCopy || null,
     nidCopy: nidCopy || null,
     customerImage: customerImage || null,

      
      // Passport information
      passportNumber: passportNumber || null,
      passportType: passportType || null,
      issueDate: issueDate || null,
      expiryDate: expiryDate || null,
      dateOfBirth: dateOfBirth || null,
      nidNumber: nidNumber || null,
      passportFirstName: passportFirstName || null,
      passportLastName: passportLastName || null,
      nationality: nationality || null,
      previousPassport: previousPassport || null,
      gender: gender || null,
      
      // Family details
      fatherName: fatherName || null,
      motherName: motherName || null,
      spouseName: spouseName || null,
      maritalStatus: maritalStatus || null,
      
      // Additional information
      notes: notes || null,
      referenceBy: referenceBy || null,
      referenceCustomerId: referenceCustomerId || null,
      
      // System fields
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Insert customer
    const result = await airCustomers.insertOne(customerData);

    res.status(201).json({
      success: true,
      message: "Customer created successfully",
      customer: {
        _id: result.insertedId,
        ...customerData
      }
    });

  } catch (error) {
    console.error('Create customer error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while creating customer",
      error: error.message
    });
  }
});

// GET: Get all Air Customers with search and pagination
app.get("/api/airCustomers", async (req, res) => {
  try {
    const { 
      search, 
      page = 1, 
      limit = 50,
      customerType,
      isActive = 'true'
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    // Build query
    const query = {};

    // Filter by isActive
    if (isActive === 'true') {
      query.isActive = true;
    } else if (isActive === 'false') {
      query.isActive = false;
    } else {
      query.isActive = { $ne: false }; // Default: active only
    }

    // Filter by customer type
    if (customerType) {
      query.customerType = customerType;
    }

    // Search functionality
    if (search && search.trim()) {
      const searchRegex = { $regex: search.trim(), $options: 'i' };
      query.$or = [
        { name: searchRegex },
        { firstName: searchRegex },
        { lastName: searchRegex },
        { mobile: searchRegex },
        { email: searchRegex },
        { passportNumber: searchRegex },
        { customerId: searchRegex },
        { nidNumber: searchRegex }
      ];
    }

    // Get total count
    const total = await airCustomers.countDocuments(query);

    // Get customers
    const customers = await airCustomers
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      customers,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get customers error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while fetching customers",
      error: error.message
    });
  }
});

// GET: Get single Air Customer by ID
app.get("/api/airCustomers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Try to find by customerId first, then by _id
    let customer = null;
    
    // Try by customerId
    customer = await airCustomers.findOne({
      customerId: id,
      isActive: { $ne: false }
    });

    // If not found, try by _id
    if (!customer && ObjectId.isValid(id)) {
      customer = await airCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Ensure financial fields are included (default to 0 if not set)
    const customerWithFinancials = {
      ...customer,
      totalAmount: customer.totalAmount !== undefined ? customer.totalAmount : 0,
      paidAmount: customer.paidAmount !== undefined ? customer.paidAmount : 0,
      totalDue: customer.totalDue !== undefined ? customer.totalDue : 0
    };

    res.json({
      success: true,
      customer: customerWithFinancials
    });

  } catch (error) {
    console.error('Get customer error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while fetching customer",
      error: error.message
    });
  }
});

// PUT: Update Air Customer
app.put("/api/airCustomers/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Find customer
    let customer = null;
    
    // Try by customerId first
    customer = await airCustomers.findOne({
      customerId: id,
      isActive: { $ne: false }
    });

    // If not found, try by _id
    if (!customer && ObjectId.isValid(id)) {
      customer = await airCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Validate mobile number if being updated
    if (updateData.mobile) {
      const mobileRegex = /^01[3-9]\d{8}$/;
      if (!mobileRegex.test(updateData.mobile)) {
        return res.status(400).json({
          success: false,
          message: "Invalid mobile number format. Please use format: 01XXXXXXXXX"
        });
      }

      // Check if mobile number already exists for another customer
      const existingCustomer = await airCustomers.findOne({
        mobile: updateData.mobile,
        customerId: { $ne: customer.customerId },
        isActive: { $ne: false }
      });

      if (existingCustomer) {
        return res.status(400).json({
          success: false,
          message: "Mobile number already exists for another customer"
        });
      }
    }

    // Validate email if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email)) {
        return res.status(400).json({
          success: false,
          message: "Invalid email format"
        });
      }
    }

    // Update name if firstName or lastName changed
    if (updateData.firstName || updateData.lastName) {
      const firstName = updateData.firstName !== undefined ? updateData.firstName : customer.firstName;
      const lastName = updateData.lastName !== undefined ? updateData.lastName : customer.lastName;
      updateData.name = `${firstName || ''}${lastName ? ' ' + lastName : ''}`.trim();
    }

    // Prepare update object (exclude _id and customerId from updates)
    const { _id, customerId, ...allowedUpdates } = updateData;
    
    // Add updatedAt
    allowedUpdates.updatedAt = new Date();

    // Update customer
    const updateQuery = customer._id 
      ? { _id: customer._id }
      : { customerId: customer.customerId };

    const result = await airCustomers.updateOne(
      updateQuery,
      { $set: allowedUpdates }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Get updated customer
    const updatedCustomer = await airCustomers.findOne(updateQuery);

    res.json({
      success: true,
      message: "Customer updated successfully",
      customer: updatedCustomer
    });

  } catch (error) {
    console.error('Update customer error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while updating customer",
      error: error.message
    });
  }
});

// PATCH: Partially update Air Customer
app.patch("/api/airCustomers/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Find customer
    let customer = null;
    
    // Try by customerId first
    customer = await airCustomers.findOne({
      customerId: id,
      isActive: { $ne: false }
    });

    // If not found, try by _id
    if (!customer && ObjectId.isValid(id)) {
      customer = await airCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Validate mobile number if being updated
    if (updateData.mobile) {
      const mobileRegex = /^01[3-9]\d{8}$/;
      if (!mobileRegex.test(updateData.mobile)) {
        return res.status(400).json({
          success: false,
          message: "Invalid mobile number format. Please use format: 01XXXXXXXXX"
        });
      }

      // Check if mobile number already exists for another customer
      const existingCustomer = await airCustomers.findOne({
        mobile: updateData.mobile,
        customerId: { $ne: customer.customerId },
        isActive: { $ne: false }
      });

      if (existingCustomer) {
        return res.status(400).json({
          success: false,
          message: "Mobile number already exists for another customer"
        });
      }
    }

    // Validate email if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email)) {
        return res.status(400).json({
          success: false,
          message: "Invalid email format"
        });
      }
    }

    // Update name if firstName or lastName changed
    if (updateData.firstName !== undefined || updateData.lastName !== undefined) {
      const firstName = updateData.firstName !== undefined ? updateData.firstName : customer.firstName;
      const lastName = updateData.lastName !== undefined ? updateData.lastName : customer.lastName;
      updateData.name = `${firstName || ''}${lastName ? ' ' + lastName : ''}`.trim();
    }

    // Prepare update object (exclude _id and customerId from updates)
    const { _id, customerId, ...allowedUpdates } = updateData;
    
    // Add updatedAt
    allowedUpdates.updatedAt = new Date();

    // Update customer
    const updateQuery = customer._id 
      ? { _id: customer._id }
      : { customerId: customer.customerId };

    const result = await airCustomers.updateOne(
      updateQuery,
      { $set: allowedUpdates }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Get updated customer
    const updatedCustomer = await airCustomers.findOne(updateQuery);

    res.json({
      success: true,
      message: "Customer updated successfully",
      customer: updatedCustomer
    });

  } catch (error) {
    console.error('Update customer error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while updating customer",
      error: error.message
    });
  }
});

// DELETE: Soft delete Air Customer
app.delete("/api/airCustomers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Find customer
    let customer = null;
    
    // Try by customerId first
    customer = await airCustomers.findOne({
      customerId: id,
      isActive: { $ne: false }
    });

    // If not found, try by _id
    if (!customer && ObjectId.isValid(id)) {
      customer = await airCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    // Soft delete (set isActive to false)
    const updateQuery = customer._id 
      ? { _id: customer._id }
      : { customerId: customer.customerId };

    const result = await airCustomers.updateOne(
      updateQuery,
      { 
        $set: { 
          isActive: false,
          updatedAt: new Date()
        } 
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        message: "Customer not found"
      });
    }

    res.json({
      success: true,
      message: "Customer deleted successfully"
    });

  } catch (error) {
    console.error('Delete customer error:', error);
    res.status(500).json({
      success: false,
      message: "Internal server error while deleting customer",
      error: error.message
    });
  }
});


// ==================== OTHER SERVICE CUSTOMER ROUTES ====================

// POST: Create new Other Service Customer
app.post("/api/other/customers", async (req, res) => {
  try {
    const {
      firstName,
      lastName,
      name,
      phone,
      email,
      address,
      city,
      country,
      status = 'active',
      notes
    } = req.body;

    // Validation - firstName, lastName, and phone are required
    if (!firstName || !firstName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "First name is required"
      });
    }

    if (!lastName || !lastName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Last name is required"
      });
    }

    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    // Validate phone number format (flexible: supports international and local formats)
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(phone.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Check if phone number already exists
    const existingCustomer = await otherCustomers.findOne({
      phone: phone.trim(),
      isActive: { $ne: false }
    });

    if (existingCustomer) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number already exists for another customer"
      });
    }

    // Generate full name from firstName and lastName if name not provided
    const fullName = name || `${firstName.trim()} ${lastName.trim()}`.trim();

    // Generate unique customer ID
    const counterDoc = await counters.findOneAndUpdate(
      { _id: 'otherServiceCustomerId' },
      { $inc: { sequence: 1 } },
      { upsert: true, returnDocument: 'after' }
    );
    const customerId = `OSC-${String(counterDoc.sequence).padStart(4, '0')}`;

    // Create customer document
    const now = new Date();
    const customerDoc = {
      customerId: customerId,
      id: customerId, // Alias for compatibility
      firstName: firstName.trim(),
      lastName: lastName.trim(),
      name: fullName,
      phone: phone.trim(),
      email: email ? email.trim() : '',
      address: address ? address.trim() : '',
      city: city ? city.trim() : '',
      country: country ? country.trim() : '',
      status: status || 'active',
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert customer
    const result = await otherCustomers.insertOne(customerDoc);

    // Return created customer
    const createdCustomer = await otherCustomers.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Customer created successfully",
      data: createdCustomer
    });

  } catch (error) {
    console.error('Create other service customer error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating customer",
      details: error.message
    });
  }
});

// GET: Get all Other Service Customers with pagination and search
app.get("/api/other/customers", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      status 
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by status if provided
    if (status) {
      filter.status = status;
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { customerId: { $regex: searchText, $options: 'i' } },
        { id: { $regex: searchText, $options: 'i' } },
        { firstName: { $regex: searchText, $options: 'i' } },
        { lastName: { $regex: searchText, $options: 'i' } },
        { name: { $regex: searchText, $options: 'i' } },
        { phone: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { city: { $regex: searchText, $options: 'i' } },
        { country: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      otherCustomers.countDocuments(filter),
      otherCustomers
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get other service customers error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching customers",
      details: error.message
    });
  }
});

// GET: Get single Other Service Customer by ID
app.get("/api/other/customers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    let customer;

    // Try to find by MongoDB ObjectId first
    if (ObjectId.isValid(id)) {
      customer = await otherCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }

    // If not found, try to find by customerId
    if (!customer) {
      customer = await otherCustomers.findOne({
        $or: [
          { customerId: id },
          { id: id }
        ],
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Customer not found"
      });
    }

    res.json({
      success: true,
      data: customer
    });

  } catch (error) {
    console.error('Get other service customer error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching customer",
      details: error.message
    });
  }
});

// PUT: Update Other Service Customer
app.put("/api/other/customers/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Find customer by MongoDB _id or customerId
    let customer;
    if (ObjectId.isValid(id)) {
      customer = await otherCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }
    
    if (!customer) {
      customer = await otherCustomers.findOne({
        $or: [
          { customerId: id },
          { id: id }
        ],
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Customer not found"
      });
    }

    // Validate phone number if being updated
    if (updateData.phone) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.phone.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid phone number format"
        });
      }

      // Check if phone number already exists for another customer
      const existingCustomer = await otherCustomers.findOne({
        phone: updateData.phone.trim(),
        _id: { $ne: customer._id },
        isActive: { $ne: false }
      });

      if (existingCustomer) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Phone number already exists for another customer"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.firstName !== undefined) {
      update.$set.firstName = String(updateData.firstName).trim();
    }
    if (updateData.lastName !== undefined) {
      update.$set.lastName = String(updateData.lastName).trim();
    }
    if (updateData.name !== undefined) {
      update.$set.name = String(updateData.name).trim();
    } else if (updateData.firstName !== undefined || updateData.lastName !== undefined) {
      // Auto-generate name if firstName or lastName changed but name not provided
      const firstName = updateData.firstName !== undefined 
        ? updateData.firstName.trim() 
        : customer.firstName;
      const lastName = updateData.lastName !== undefined 
        ? updateData.lastName.trim() 
        : customer.lastName;
      update.$set.name = `${firstName} ${lastName}`.trim();
    }
    if (updateData.phone !== undefined) {
      update.$set.phone = String(updateData.phone).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.address !== undefined) {
      update.$set.address = updateData.address ? String(updateData.address).trim() : '';
    }
    if (updateData.city !== undefined) {
      update.$set.city = updateData.city ? String(updateData.city).trim() : '';
    }
    if (updateData.country !== undefined) {
      update.$set.country = updateData.country ? String(updateData.country).trim() : '';
    }
    if (updateData.status !== undefined) {
      update.$set.status = String(updateData.status);
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Update customer
    const result = await otherCustomers.updateOne(
      { _id: customer._id },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Customer not found"
      });
    }

    // Get updated customer
    const updatedCustomer = await otherCustomers.findOne({ _id: customer._id });

    res.json({
      success: true,
      message: "Customer updated successfully",
      data: updatedCustomer
    });

  } catch (error) {
    console.error('Update other service customer error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating customer",
      details: error.message
    });
  }
});

// DELETE: Delete Other Service Customer (Soft delete)
app.delete("/api/other/customers/:id", async (req, res) => {
  try {
    const { id } = req.params;

    // Find customer by MongoDB _id or customerId
    let customer;
    if (ObjectId.isValid(id)) {
      customer = await otherCustomers.findOne({
        _id: new ObjectId(id),
        isActive: { $ne: false }
      });
    }
    
    if (!customer) {
      customer = await otherCustomers.findOne({
        $or: [
          { customerId: id },
          { id: id }
        ],
        isActive: { $ne: false }
      });
    }

    if (!customer) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Customer not found"
      });
    }

    // Soft delete
    await otherCustomers.updateOne(
      { _id: customer._id },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Customer deleted successfully"
    });

  } catch (error) {
    console.error('Delete other service customer error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting customer",
      details: error.message
    });
  }
});


// ==================== PASSPORT SERVICE ROUTES ====================

// POST: Create new Passport Service
app.post("/api/passport-services", async (req, res) => {
  try {
    const {
      clientId,
      clientName,
      serviceType = 'new_passport',
      phone,
      email,
      address,
      date,
      status = 'pending',
      notes,
      expectedDeliveryDate,
      applicationNumber,
      dateOfBirth,
      validity,
      pages,
      deliveryType,
      officeContactPersonId,
      officeContactPersonName,
      passportFees = 0,
      bankCharges = 0,
      vendorFees = 0,
      formFillupCharge = 0,
      totalBill = 0
    } = req.body;

    // Validation - clientName, phone, and date are required
    if (!clientName || !clientName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Client name is required"
      });
    }

    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    if (!date) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Date is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(phone.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate service type
    const validServiceTypes = ['new_passport', 'renewal', 'replacement', 'visa_stamping', 'other'];
    if (!validServiceTypes.includes(serviceType)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid service type"
      });
    }

    // Validate status
    const validStatuses = ['pending', 'in_process', 'completed', 'cancelled'];
    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid status"
      });
    }

    // Calculate total bill if not provided
    const calculatedTotal = (parseFloat(passportFees) || 0) + 
                           (parseFloat(bankCharges) || 0) + 
                           (parseFloat(vendorFees) || 0) + 
                           (parseFloat(formFillupCharge) || 0);
    const finalTotalBill = totalBill || calculatedTotal;

    // Create passport service document
    const now = new Date();
    const serviceDoc = {
      clientId: clientId || null,
      clientName: clientName.trim(),
      serviceType: serviceType,
      phone: phone.trim(),
      email: email ? email.trim() : '',
      address: address ? address.trim() : '',
      date: new Date(date),
      status: status,
      notes: notes ? notes.trim() : '',
      expectedDeliveryDate: expectedDeliveryDate ? new Date(expectedDeliveryDate) : null,
      applicationNumber: applicationNumber ? applicationNumber.trim() : '',
      dateOfBirth: dateOfBirth ? new Date(dateOfBirth) : null,
      validity: validity || '',
      pages: pages || '',
      deliveryType: deliveryType || '',
      officeContactPersonId: officeContactPersonId || null,
      officeContactPersonName: officeContactPersonName ? officeContactPersonName.trim() : '',
      passportFees: parseFloat(passportFees) || 0,
      bankCharges: parseFloat(bankCharges) || 0,
      vendorFees: parseFloat(vendorFees) || 0,
      formFillupCharge: parseFloat(formFillupCharge) || 0,
      totalBill: parseFloat(finalTotalBill),
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert passport service
    const result = await passportServices.insertOne(serviceDoc);

    // Return created service
    const createdService = await passportServices.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Passport service created successfully",
      data: createdService
    });

  } catch (error) {
    console.error('Create passport service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating passport service",
      details: error.message
    });
  }
});

// GET: Get all Passport Services with pagination and search
app.get("/api/passport-services", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      status,
      serviceType,
      clientId,
      dateFrom,
      dateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by status if provided
    if (status) {
      filter.status = status;
    }

    // Filter by service type if provided
    if (serviceType) {
      filter.serviceType = serviceType;
    }

    // Filter by client ID if provided
    if (clientId) {
      filter.clientId = clientId;
    }

    // Date range filter
    if (dateFrom || dateTo) {
      filter.date = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        start.setHours(0, 0, 0, 0);
        filter.date.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        end.setHours(23, 59, 59, 999);
        filter.date.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { clientName: { $regex: searchText, $options: 'i' } },
        { phone: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { applicationNumber: { $regex: searchText, $options: 'i' } },
        { address: { $regex: searchText, $options: 'i' } },
        { officeContactPersonName: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      passportServices.countDocuments(filter),
      passportServices
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get passport services error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching passport services",
      details: error.message
    });
  }
});

// GET: Get single Passport Service by ID
app.get("/api/passport-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid passport service ID"
      });
    }

    const service = await passportServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Passport service not found"
      });
    }

    res.json({
      success: true,
      data: service
    });

  } catch (error) {
    console.error('Get passport service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching passport service",
      details: error.message
    });
  }
});

// PUT: Update Passport Service
app.put("/api/passport-services/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid passport service ID"
      });
    }

    // Find service
    const service = await passportServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Passport service not found"
      });
    }

    // Validate phone number if being updated
    if (updateData.phone) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.phone.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid phone number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate service type if being updated
    if (updateData.serviceType) {
      const validServiceTypes = ['new_passport', 'renewal', 'replacement', 'visa_stamping', 'other'];
      if (!validServiceTypes.includes(updateData.serviceType)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid service type"
        });
      }
    }

    // Validate status if being updated
    if (updateData.status) {
      const validStatuses = ['pending', 'in_process', 'completed', 'cancelled'];
      if (!validStatuses.includes(updateData.status)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid status"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.clientId !== undefined) {
      update.$set.clientId = updateData.clientId || null;
    }
    if (updateData.clientName !== undefined) {
      update.$set.clientName = String(updateData.clientName).trim();
    }
    if (updateData.serviceType !== undefined) {
      update.$set.serviceType = String(updateData.serviceType);
    }
    if (updateData.phone !== undefined) {
      update.$set.phone = String(updateData.phone).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.address !== undefined) {
      update.$set.address = updateData.address ? String(updateData.address).trim() : '';
    }
    if (updateData.date !== undefined) {
      update.$set.date = new Date(updateData.date);
    }
    if (updateData.status !== undefined) {
      update.$set.status = String(updateData.status);
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }
    if (updateData.expectedDeliveryDate !== undefined) {
      update.$set.expectedDeliveryDate = updateData.expectedDeliveryDate ? new Date(updateData.expectedDeliveryDate) : null;
    }
    if (updateData.applicationNumber !== undefined) {
      update.$set.applicationNumber = updateData.applicationNumber ? String(updateData.applicationNumber).trim() : '';
    }
    if (updateData.dateOfBirth !== undefined) {
      update.$set.dateOfBirth = updateData.dateOfBirth ? new Date(updateData.dateOfBirth) : null;
    }
    if (updateData.validity !== undefined) {
      update.$set.validity = updateData.validity ? String(updateData.validity) : '';
    }
    if (updateData.pages !== undefined) {
      update.$set.pages = updateData.pages ? String(updateData.pages) : '';
    }
    if (updateData.deliveryType !== undefined) {
      update.$set.deliveryType = updateData.deliveryType ? String(updateData.deliveryType) : '';
    }
    if (updateData.officeContactPersonId !== undefined) {
      update.$set.officeContactPersonId = updateData.officeContactPersonId || null;
    }
    if (updateData.officeContactPersonName !== undefined) {
      update.$set.officeContactPersonName = updateData.officeContactPersonName ? String(updateData.officeContactPersonName).trim() : '';
    }
    if (updateData.passportFees !== undefined) {
      update.$set.passportFees = parseFloat(updateData.passportFees) || 0;
    }
    if (updateData.bankCharges !== undefined) {
      update.$set.bankCharges = parseFloat(updateData.bankCharges) || 0;
    }
    if (updateData.vendorFees !== undefined) {
      update.$set.vendorFees = parseFloat(updateData.vendorFees) || 0;
    }
    if (updateData.formFillupCharge !== undefined) {
      update.$set.formFillupCharge = parseFloat(updateData.formFillupCharge) || 0;
    }

    // Recalculate total bill if any fee field is updated
    if (updateData.passportFees !== undefined || 
        updateData.bankCharges !== undefined || 
        updateData.vendorFees !== undefined || 
        updateData.formFillupCharge !== undefined ||
        updateData.totalBill !== undefined) {
      
      const passportFees = updateData.passportFees !== undefined 
        ? parseFloat(updateData.passportFees) || 0 
        : service.passportFees || 0;
      const bankCharges = updateData.bankCharges !== undefined 
        ? parseFloat(updateData.bankCharges) || 0 
        : service.bankCharges || 0;
      const vendorFees = updateData.vendorFees !== undefined 
        ? parseFloat(updateData.vendorFees) || 0 
        : service.vendorFees || 0;
      const formFillupCharge = updateData.formFillupCharge !== undefined 
        ? parseFloat(updateData.formFillupCharge) || 0 
        : service.formFillupCharge || 0;
      
      const calculatedTotal = passportFees + bankCharges + vendorFees + formFillupCharge;
      update.$set.totalBill = updateData.totalBill !== undefined 
        ? parseFloat(updateData.totalBill) 
        : calculatedTotal;
    }

    // Update service
    const result = await passportServices.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Passport service not found"
      });
    }

    // Get updated service
    const updatedService = await passportServices.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Passport service updated successfully",
      data: updatedService
    });

  } catch (error) {
    console.error('Update passport service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating passport service",
      details: error.message
    });
  }
});

// DELETE: Delete Passport Service (Soft delete)
app.delete("/api/passport-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid passport service ID"
      });
    }

    // Check if service exists
    const service = await passportServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Passport service not found"
      });
    }

    // Soft delete
    await passportServices.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Passport service deleted successfully"
    });

  } catch (error) {
    console.error('Delete passport service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting passport service",
      details: error.message
    });
  }
});


// ==================== MANPOWER SERVICE ROUTES ====================

// POST: Create new Manpower Service
app.post("/api/manpower-services", async (req, res) => {
  try {
    const {
      clientId,
      clientName,
      serviceType = 'recruitment',
      phone,
      email,
      address,
      appliedDate,
      expectedDeliveryDate,
      vendorId,
      vendorName,
      vendorBill = 0,
      othersBill = 0,
      serviceCharge = 0,
      totalBill = 0,
      status = 'active',
      notes
    } = req.body;

    // Validation - clientName, phone, and appliedDate are required
    if (!clientName || !clientName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Client name is required"
      });
    }

    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    if (!appliedDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Applied date is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(phone.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate service type
    const validServiceTypes = ['recruitment', 'placement', 'training', 'consultation', 'other'];
    if (!validServiceTypes.includes(serviceType)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid service type"
      });
    }

    // Validate status
    const validStatuses = ['active', 'in_process', 'completed', 'cancelled'];
    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid status"
      });
    }

    // Calculate total bill if not provided
    const calculatedTotal = (parseFloat(vendorBill) || 0) + 
                           (parseFloat(othersBill) || 0) + 
                           (parseFloat(serviceCharge) || 0);
    const finalTotalBill = totalBill || calculatedTotal;

    // Create manpower service document
    const now = new Date();
    const serviceDoc = {
      clientId: clientId || null,
      clientName: clientName.trim(),
      serviceType: serviceType,
      phone: phone.trim(),
      email: email ? email.trim() : '',
      address: address ? address.trim() : '',
      appliedDate: new Date(appliedDate),
      expectedDeliveryDate: expectedDeliveryDate ? new Date(expectedDeliveryDate) : null,
      vendorId: vendorId || null,
      vendorName: vendorName ? vendorName.trim() : '',
      vendorBill: parseFloat(vendorBill) || 0,
      othersBill: parseFloat(othersBill) || 0,
      serviceCharge: parseFloat(serviceCharge) || 0,
      totalBill: parseFloat(finalTotalBill),
      status: status,
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert manpower service
    const result = await manpowerServices.insertOne(serviceDoc);

    // Return created service
    const createdService = await manpowerServices.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Manpower service created successfully",
      data: createdService
    });

  } catch (error) {
    console.error('Create manpower service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating manpower service",
      details: error.message
    });
  }
});

// GET: Get all Manpower Services with pagination and search
app.get("/api/manpower-services", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      status,
      serviceType,
      clientId,
      vendorId,
      appliedDateFrom,
      appliedDateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by status if provided
    if (status) {
      filter.status = status;
    }

    // Filter by service type if provided
    if (serviceType) {
      filter.serviceType = serviceType;
    }

    // Filter by client ID if provided
    if (clientId) {
      filter.clientId = clientId;
    }

    // Filter by vendor ID if provided
    if (vendorId) {
      filter.vendorId = vendorId;
    }

    // Applied date range filter
    if (appliedDateFrom || appliedDateTo) {
      filter.appliedDate = {};
      if (appliedDateFrom) {
        const start = new Date(appliedDateFrom);
        start.setHours(0, 0, 0, 0);
        filter.appliedDate.$gte = start;
      }
      if (appliedDateTo) {
        const end = new Date(appliedDateTo);
        end.setHours(23, 59, 59, 999);
        filter.appliedDate.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { clientName: { $regex: searchText, $options: 'i' } },
        { phone: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { address: { $regex: searchText, $options: 'i' } },
        { vendorName: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      manpowerServices.countDocuments(filter),
      manpowerServices
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get manpower services error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching manpower services",
      details: error.message
    });
  }
});

// GET: Get single Manpower Service by ID
app.get("/api/manpower-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid manpower service ID"
      });
    }

    const service = await manpowerServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Manpower service not found"
      });
    }

    res.json({
      success: true,
      data: service
    });

  } catch (error) {
    console.error('Get manpower service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching manpower service",
      details: error.message
    });
  }
});

// PUT: Update Manpower Service
app.put("/api/manpower-services/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid manpower service ID"
      });
    }

    // Find service
    const service = await manpowerServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Manpower service not found"
      });
    }

    // Validate phone number if being updated
    if (updateData.phone) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.phone.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid phone number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate service type if being updated
    if (updateData.serviceType) {
      const validServiceTypes = ['recruitment', 'placement', 'training', 'consultation', 'other'];
      if (!validServiceTypes.includes(updateData.serviceType)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid service type"
        });
      }
    }

    // Validate status if being updated
    if (updateData.status) {
      const validStatuses = ['active', 'in_process', 'completed', 'cancelled'];
      if (!validStatuses.includes(updateData.status)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid status"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.clientId !== undefined) {
      update.$set.clientId = updateData.clientId || null;
    }
    if (updateData.clientName !== undefined) {
      update.$set.clientName = String(updateData.clientName).trim();
    }
    if (updateData.serviceType !== undefined) {
      update.$set.serviceType = String(updateData.serviceType);
    }
    if (updateData.phone !== undefined) {
      update.$set.phone = String(updateData.phone).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.address !== undefined) {
      update.$set.address = updateData.address ? String(updateData.address).trim() : '';
    }
    if (updateData.appliedDate !== undefined) {
      update.$set.appliedDate = new Date(updateData.appliedDate);
    }
    if (updateData.expectedDeliveryDate !== undefined) {
      update.$set.expectedDeliveryDate = updateData.expectedDeliveryDate ? new Date(updateData.expectedDeliveryDate) : null;
    }
    if (updateData.vendorId !== undefined) {
      update.$set.vendorId = updateData.vendorId || null;
    }
    if (updateData.vendorName !== undefined) {
      update.$set.vendorName = updateData.vendorName ? String(updateData.vendorName).trim() : '';
    }
    if (updateData.vendorBill !== undefined) {
      update.$set.vendorBill = parseFloat(updateData.vendorBill) || 0;
    }
    if (updateData.othersBill !== undefined) {
      update.$set.othersBill = parseFloat(updateData.othersBill) || 0;
    }
    if (updateData.serviceCharge !== undefined) {
      update.$set.serviceCharge = parseFloat(updateData.serviceCharge) || 0;
    }
    if (updateData.status !== undefined) {
      update.$set.status = String(updateData.status);
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Recalculate total bill if any bill field is updated
    if (updateData.vendorBill !== undefined || 
        updateData.othersBill !== undefined || 
        updateData.serviceCharge !== undefined ||
        updateData.totalBill !== undefined) {
      
      const vendorBill = updateData.vendorBill !== undefined 
        ? parseFloat(updateData.vendorBill) || 0 
        : service.vendorBill || 0;
      const othersBill = updateData.othersBill !== undefined 
        ? parseFloat(updateData.othersBill) || 0 
        : service.othersBill || 0;
      const serviceCharge = updateData.serviceCharge !== undefined 
        ? parseFloat(updateData.serviceCharge) || 0 
        : service.serviceCharge || 0;
      
      const calculatedTotal = vendorBill + othersBill + serviceCharge;
      update.$set.totalBill = updateData.totalBill !== undefined 
        ? parseFloat(updateData.totalBill) 
        : calculatedTotal;
    }

    // Update service
    const result = await manpowerServices.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Manpower service not found"
      });
    }

    // Get updated service
    const updatedService = await manpowerServices.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Manpower service updated successfully",
      data: updatedService
    });

  } catch (error) {
    console.error('Update manpower service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating manpower service",
      details: error.message
    });
  }
});

// DELETE: Delete Manpower Service (Soft delete)
app.delete("/api/manpower-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid manpower service ID"
      });
    }

    // Check if service exists
    const service = await manpowerServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Manpower service not found"
      });
    }

    // Soft delete
    await manpowerServices.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Manpower service deleted successfully"
    });

  } catch (error) {
    console.error('Delete manpower service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting manpower service",
      details: error.message
    });
  }
});


// ==================== VISA PROCESSING SERVICE ROUTES ====================

// POST: Create new Visa Processing Service
app.post("/api/visa-processing-services", async (req, res) => {
  try {
    const {
      clientId,
      clientName,
      applicantName,
      country,
      visaType = 'tourist',
      passportNumber,
      phone,
      email,
      address,
      date,
      appliedDate,
      expectedDeliveryDate,
      vendorId,
      vendorName,
      vendorBill = 0,
      othersBill = 0,
      totalBill = 0,
      status = 'pending',
      notes
    } = req.body;

    // Validation - clientName, phone, and date are required
    if (!clientName || !clientName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Client name is required"
      });
    }

    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    const serviceDate = date || appliedDate;
    if (!serviceDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Applied date is required"
      });
    }

    if (!country || !country.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Country is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(phone.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate visa type
    const validVisaTypes = ['tourist', 'business', 'student', 'work', 'transit', 'medical', 'other'];
    if (visaType && !validVisaTypes.includes(visaType)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid visa type"
      });
    }

    // Validate status
    const validStatuses = ['pending', 'processing', 'in_process', 'approved', 'rejected', 'completed', 'cancelled'];
    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid status"
      });
    }

    // Calculate total bill if not provided
    const calculatedTotal = (parseFloat(vendorBill) || 0) + (parseFloat(othersBill) || 0);
    const finalTotalBill = totalBill || calculatedTotal;

    // Create visa processing service document
    const now = new Date();
    const serviceDoc = {
      clientId: clientId || null,
      clientName: clientName.trim(),
      applicantName: applicantName ? applicantName.trim() : clientName.trim(),
      country: country.trim(),
      visaType: visaType,
      passportNumber: passportNumber ? passportNumber.trim() : '',
      phone: phone.trim(),
      email: email ? email.trim() : '',
      address: address ? address.trim() : '',
      appliedDate: new Date(serviceDate),
      expectedDeliveryDate: expectedDeliveryDate ? new Date(expectedDeliveryDate) : null,
      vendorId: vendorId || null,
      vendorName: vendorName ? vendorName.trim() : '',
      vendorBill: parseFloat(vendorBill) || 0,
      othersBill: parseFloat(othersBill) || 0,
      totalBill: parseFloat(finalTotalBill),
      status: status,
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert visa processing service
    const result = await visaProcessingServices.insertOne(serviceDoc);

    // Return created service
    const createdService = await visaProcessingServices.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Visa processing service created successfully",
      data: createdService
    });

  } catch (error) {
    console.error('Create visa processing service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating visa processing service",
      details: error.message
    });
  }
});

// GET: Get all Visa Processing Services with pagination and search
app.get("/api/visa-processing-services", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      status,
      visaType,
      country,
      clientId,
      vendorId,
      dateFrom,
      dateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by status if provided
    if (status) {
      filter.status = status;
    }

    // Filter by visa type if provided
    if (visaType) {
      filter.visaType = visaType;
    }

    // Filter by country if provided
    if (country) {
      filter.country = { $regex: country, $options: 'i' };
    }

    // Filter by client ID if provided
    if (clientId) {
      filter.clientId = clientId;
    }

    // Filter by vendor ID if provided
    if (vendorId) {
      filter.vendorId = vendorId;
    }

    // Date range filter
    if (dateFrom || dateTo) {
      filter.appliedDate = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        start.setHours(0, 0, 0, 0);
        filter.appliedDate.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        end.setHours(23, 59, 59, 999);
        filter.appliedDate.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { clientName: { $regex: searchText, $options: 'i' } },
        { applicantName: { $regex: searchText, $options: 'i' } },
        { phone: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { passportNumber: { $regex: searchText, $options: 'i' } },
        { country: { $regex: searchText, $options: 'i' } },
        { vendorName: { $regex: searchText, $options: 'i' } },
        { address: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      visaProcessingServices.countDocuments(filter),
      visaProcessingServices
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get visa processing services error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching visa processing services",
      details: error.message
    });
  }
});

// GET: Get single Visa Processing Service by ID
app.get("/api/visa-processing-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid visa processing service ID"
      });
    }

    const service = await visaProcessingServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Visa processing service not found"
      });
    }

    res.json({
      success: true,
      data: service
    });

  } catch (error) {
    console.error('Get visa processing service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching visa processing service",
      details: error.message
    });
  }
});

// PUT: Update Visa Processing Service
app.put("/api/visa-processing-services/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid visa processing service ID"
      });
    }

    // Find service
    const service = await visaProcessingServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Visa processing service not found"
      });
    }

    // Validate phone number if being updated
    if (updateData.phone) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.phone.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid phone number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate visa type if being updated
    if (updateData.visaType) {
      const validVisaTypes = ['tourist', 'business', 'student', 'work', 'transit', 'medical', 'other'];
      if (!validVisaTypes.includes(updateData.visaType)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid visa type"
        });
      }
    }

    // Validate status if being updated
    if (updateData.status) {
      const validStatuses = ['pending', 'processing', 'in_process', 'approved', 'rejected', 'completed', 'cancelled'];
      if (!validStatuses.includes(updateData.status)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid status"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.clientId !== undefined) {
      update.$set.clientId = updateData.clientId || null;
    }
    if (updateData.clientName !== undefined) {
      update.$set.clientName = String(updateData.clientName).trim();
    }
    if (updateData.applicantName !== undefined) {
      update.$set.applicantName = String(updateData.applicantName).trim();
    }
    if (updateData.country !== undefined) {
      update.$set.country = String(updateData.country).trim();
    }
    if (updateData.visaType !== undefined) {
      update.$set.visaType = String(updateData.visaType);
    }
    if (updateData.passportNumber !== undefined) {
      update.$set.passportNumber = updateData.passportNumber ? String(updateData.passportNumber).trim() : '';
    }
    if (updateData.phone !== undefined) {
      update.$set.phone = String(updateData.phone).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.address !== undefined) {
      update.$set.address = updateData.address ? String(updateData.address).trim() : '';
    }
    if (updateData.appliedDate !== undefined) {
      update.$set.appliedDate = new Date(updateData.appliedDate);
    }
    if (updateData.date !== undefined) {
      update.$set.appliedDate = new Date(updateData.date);
    }
    if (updateData.expectedDeliveryDate !== undefined) {
      update.$set.expectedDeliveryDate = updateData.expectedDeliveryDate ? new Date(updateData.expectedDeliveryDate) : null;
    }
    if (updateData.vendorId !== undefined) {
      update.$set.vendorId = updateData.vendorId || null;
    }
    if (updateData.vendorName !== undefined) {
      update.$set.vendorName = updateData.vendorName ? String(updateData.vendorName).trim() : '';
    }
    if (updateData.vendorBill !== undefined) {
      update.$set.vendorBill = parseFloat(updateData.vendorBill) || 0;
    }
    if (updateData.othersBill !== undefined) {
      update.$set.othersBill = parseFloat(updateData.othersBill) || 0;
    }
    if (updateData.status !== undefined) {
      update.$set.status = String(updateData.status);
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Recalculate total bill if any fee field is updated
    if (updateData.vendorBill !== undefined || 
        updateData.othersBill !== undefined ||
        updateData.totalBill !== undefined) {
      
      const vendorBill = updateData.vendorBill !== undefined 
        ? parseFloat(updateData.vendorBill) || 0 
        : service.vendorBill || 0;
      const othersBill = updateData.othersBill !== undefined 
        ? parseFloat(updateData.othersBill) || 0 
        : service.othersBill || 0;
      
      const calculatedTotal = vendorBill + othersBill;
      update.$set.totalBill = updateData.totalBill !== undefined 
        ? parseFloat(updateData.totalBill) 
        : calculatedTotal;
    }

    // Update service
    const result = await visaProcessingServices.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Visa processing service not found"
      });
    }

    // Get updated service
    const updatedService = await visaProcessingServices.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Visa processing service updated successfully",
      data: updatedService
    });

  } catch (error) {
    console.error('Update visa processing service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating visa processing service",
      details: error.message
    });
  }
});

// DELETE: Delete Visa Processing Service (Soft delete)
app.delete("/api/visa-processing-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid visa processing service ID"
      });
    }

    // Check if service exists
    const service = await visaProcessingServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Visa processing service not found"
      });
    }

    // Soft delete
    await visaProcessingServices.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Visa processing service deleted successfully"
    });

  } catch (error) {
    console.error('Delete visa processing service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting visa processing service",
      details: error.message
    });
  }
});


// ==================== TICKET CHECK ROUTES ====================

// POST: Create new Ticket Check
app.post("/api/ticket-checks", async (req, res) => {
  try {
    const {
      customerId,
      travelDate,
      passengerName,
      travellingCountry,
      passportNo,
      contactNo,
      isWhatsAppSame = true,
      whatsAppNo,
      airlineName,
      route,
      bookingRef,
      issuingAgentName,
      email,
      reservationOfficerId,
      reservationOfficerName,
      notes
    } = req.body;

    // Validation - Required fields
    if (!travelDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Travel date is required"
      });
    }

    if (!passengerName || !passengerName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Passenger name is required"
      });
    }

    if (!travellingCountry || !travellingCountry.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Travelling country is required"
      });
    }

    if (!passportNo || !passportNo.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Passport number is required"
      });
    }

    if (!contactNo || !contactNo.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Contact number is required"
      });
    }

    if (!isWhatsAppSame && (!whatsAppNo || !whatsAppNo.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "WhatsApp number is required"
      });
    }

    if (!airlineName || !airlineName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Airlines name is required"
      });
    }

    if (!route || !route.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Route is required"
      });
    }

    if (!bookingRef || !bookingRef.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Booking reference is required"
      });
    }

    if (!issuingAgentName || !issuingAgentName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Issuing agent name is required"
      });
    }

    if (!reservationOfficerId || !reservationOfficerId.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Reservation officer is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(contactNo.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid contact number format"
      });
    }

    // Validate WhatsApp number if different
    if (!isWhatsAppSame && whatsAppNo && whatsAppNo.trim()) {
      if (!phoneRegex.test(whatsAppNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid WhatsApp number format"
        });
      }
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Create ticket check document
    const now = new Date();
    const ticketCheckDoc = {
      customerId: customerId || null,
      travelDate: new Date(travelDate),
      passengerName: passengerName.trim(),
      travellingCountry: travellingCountry.trim(),
      passportNo: passportNo.trim(),
      contactNo: contactNo.trim(),
      isWhatsAppSame: Boolean(isWhatsAppSame),
      whatsAppNo: isWhatsAppSame ? contactNo.trim() : (whatsAppNo ? whatsAppNo.trim() : ''),
      airlineName: airlineName.trim(),
      route: route.trim(),
      bookingRef: bookingRef.trim(),
      issuingAgentName: issuingAgentName.trim(),
      email: email ? email.trim() : '',
      reservationOfficerId: reservationOfficerId.trim(),
      reservationOfficerName: reservationOfficerName ? reservationOfficerName.trim() : '',
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert ticket check
    const result = await ticketChecks.insertOne(ticketCheckDoc);

    // Return created ticket check
    const createdTicketCheck = await ticketChecks.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Ticket check created successfully",
      data: createdTicketCheck
    });

  } catch (error) {
    console.error('Create ticket check error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating ticket check",
      details: error.message
    });
  }
});

// GET: Get all Ticket Checks with pagination and search
app.get("/api/ticket-checks", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      customerId,
      reservationOfficerId,
      airlineName,
      travellingCountry,
      dateFrom,
      dateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by customer ID if provided
    if (customerId) {
      filter.customerId = customerId;
    }

    // Filter by reservation officer if provided
    if (reservationOfficerId) {
      filter.reservationOfficerId = reservationOfficerId;
    }

    // Filter by airline if provided
    if (airlineName) {
      filter.airlineName = { $regex: airlineName, $options: 'i' };
    }

    // Filter by country if provided
    if (travellingCountry) {
      filter.travellingCountry = { $regex: travellingCountry, $options: 'i' };
    }

    // Date range filter
    if (dateFrom || dateTo) {
      filter.travelDate = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        start.setHours(0, 0, 0, 0);
        filter.travelDate.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        end.setHours(23, 59, 59, 999);
        filter.travelDate.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { passengerName: { $regex: searchText, $options: 'i' } },
        { passportNo: { $regex: searchText, $options: 'i' } },
        { contactNo: { $regex: searchText, $options: 'i' } },
        { whatsAppNo: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { bookingRef: { $regex: searchText, $options: 'i' } },
        { airlineName: { $regex: searchText, $options: 'i' } },
        { route: { $regex: searchText, $options: 'i' } },
        { travellingCountry: { $regex: searchText, $options: 'i' } },
        { issuingAgentName: { $regex: searchText, $options: 'i' } },
        { reservationOfficerName: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      ticketChecks.countDocuments(filter),
      ticketChecks
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get ticket checks error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching ticket checks",
      details: error.message
    });
  }
});

// GET: Get single Ticket Check by ID
app.get("/api/ticket-checks/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid ticket check ID"
      });
    }

    const ticketCheck = await ticketChecks.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!ticketCheck) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Ticket check not found"
      });
    }

    res.json({
      success: true,
      data: ticketCheck
    });

  } catch (error) {
    console.error('Get ticket check error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching ticket check",
      details: error.message
    });
  }
});

// PUT: Update Ticket Check
app.put("/api/ticket-checks/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid ticket check ID"
      });
    }

    // Find ticket check
    const ticketCheck = await ticketChecks.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!ticketCheck) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Ticket check not found"
      });
    }

    // Validate contact number if being updated
    if (updateData.contactNo) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.contactNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid contact number format"
        });
      }
    }

    // Validate WhatsApp number if being updated
    if (updateData.whatsAppNo && updateData.whatsAppNo.trim()) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.whatsAppNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid WhatsApp number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.customerId !== undefined) {
      update.$set.customerId = updateData.customerId || null;
    }
    if (updateData.travelDate !== undefined) {
      update.$set.travelDate = new Date(updateData.travelDate);
    }
    if (updateData.passengerName !== undefined) {
      update.$set.passengerName = String(updateData.passengerName).trim();
    }
    if (updateData.travellingCountry !== undefined) {
      update.$set.travellingCountry = String(updateData.travellingCountry).trim();
    }
    if (updateData.passportNo !== undefined) {
      update.$set.passportNo = String(updateData.passportNo).trim();
    }
    if (updateData.contactNo !== undefined) {
      update.$set.contactNo = String(updateData.contactNo).trim();
    }
    if (updateData.isWhatsAppSame !== undefined) {
      update.$set.isWhatsAppSame = Boolean(updateData.isWhatsAppSame);
      // If WhatsApp is same, copy contact number
      if (updateData.isWhatsAppSame) {
        update.$set.whatsAppNo = updateData.contactNo ? String(updateData.contactNo).trim() : ticketCheck.contactNo;
      }
    }
    if (updateData.whatsAppNo !== undefined && !updateData.isWhatsAppSame) {
      update.$set.whatsAppNo = updateData.whatsAppNo ? String(updateData.whatsAppNo).trim() : '';
    }
    if (updateData.airlineName !== undefined) {
      update.$set.airlineName = String(updateData.airlineName).trim();
    }
    if (updateData.route !== undefined) {
      update.$set.route = String(updateData.route).trim();
    }
    if (updateData.bookingRef !== undefined) {
      update.$set.bookingRef = String(updateData.bookingRef).trim();
    }
    if (updateData.issuingAgentName !== undefined) {
      update.$set.issuingAgentName = String(updateData.issuingAgentName).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.reservationOfficerId !== undefined) {
      update.$set.reservationOfficerId = String(updateData.reservationOfficerId).trim();
    }
    if (updateData.reservationOfficerName !== undefined) {
      update.$set.reservationOfficerName = updateData.reservationOfficerName ? String(updateData.reservationOfficerName).trim() : '';
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Update ticket check
    const result = await ticketChecks.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Ticket check not found"
      });
    }

    // Get updated ticket check
    const updatedTicketCheck = await ticketChecks.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Ticket check updated successfully",
      data: updatedTicketCheck
    });

  } catch (error) {
    console.error('Update ticket check error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating ticket check",
      details: error.message
    });
  }
});

// DELETE: Delete Ticket Check (Soft delete)
app.delete("/api/ticket-checks/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid ticket check ID"
      });
    }

    // Check if ticket check exists
    const ticketCheck = await ticketChecks.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!ticketCheck) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Ticket check not found"
      });
    }

    // Soft delete
    await ticketChecks.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Ticket check deleted successfully"
    });

  } catch (error) {
    console.error('Delete ticket check error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting ticket check",
      details: error.message
    });
  }
});


// ==================== OLD TICKET REISSUE ROUTES ====================

// POST: Create new Old Ticket Reissue
app.post("/api/old-ticket-reissues", async (req, res) => {
  try {
    const {
      customerId,
      formDate,
      firstName,
      lastName,
      travellingCountry,
      passportNo,
      contactNo,
      isWhatsAppSame = true,
      whatsAppNo,
      airlineName,
      origin,
      destination,
      airlinesPnr,
      oldDate,
      newDate,
      reissueVendorId,
      reissueVendorName,
      vendorAmount,
      totalContractAmount,
      issuingAgentName,
      issuingAgentContact,
      agentEmail,
      reservationOfficerId,
      reservationOfficerName,
      notes
    } = req.body;

    // Validation - Required fields
    if (!formDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Form date is required"
      });
    }

    if (!firstName || !firstName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Passenger first name is required"
      });
    }

    if (!lastName || !lastName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Passenger last name is required"
      });
    }

    if (!travellingCountry || !travellingCountry.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Travelling country is required"
      });
    }

    if (!passportNo || !passportNo.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Passport number is required"
      });
    }

    if (!contactNo || !contactNo.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Contact number is required"
      });
    }

    if (!isWhatsAppSame && (!whatsAppNo || !whatsAppNo.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "WhatsApp number is required"
      });
    }

    if (!airlineName || !airlineName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Airlines name is required"
      });
    }

    if (!origin || !origin.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Origin is required"
      });
    }

    if (!destination || !destination.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Destination is required"
      });
    }

    if (!airlinesPnr || !airlinesPnr.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Airlines PNR is required"
      });
    }

    if (!oldDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Old date is required"
      });
    }

    if (!newDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "New date is required"
      });
    }

    if (!reissueVendorId || !reissueVendorId.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Reissue vendor is required"
      });
    }

    if (vendorAmount === undefined || vendorAmount === null || vendorAmount === '') {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Vendor amount is required"
      });
    }

    if (totalContractAmount === undefined || totalContractAmount === null || totalContractAmount === '') {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Total contract amount is required"
      });
    }

    if (!issuingAgentName || !issuingAgentName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Issuing agent name is required"
      });
    }

    if (!issuingAgentContact || !issuingAgentContact.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Issuing agent contact is required"
      });
    }

    if (!reservationOfficerId || !reservationOfficerId.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Reservation officer is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(contactNo.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid contact number format"
      });
    }

    // Validate agent contact format
    if (!phoneRegex.test(issuingAgentContact.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid issuing agent contact format"
      });
    }

    // Validate WhatsApp number if different
    if (!isWhatsAppSame && whatsAppNo && whatsAppNo.trim()) {
      if (!phoneRegex.test(whatsAppNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid WhatsApp number format"
        });
      }
    }

    // Validate email format if provided
    if (agentEmail && agentEmail.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(agentEmail.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Calculate profit
    const vendorAmt = parseFloat(vendorAmount) || 0;
    const totalAmt = parseFloat(totalContractAmount) || 0;
    const profit = totalAmt - vendorAmt;

    // Create old ticket reissue document
    const now = new Date();
    const reissueDoc = {
      customerId: customerId || null,
      formDate: new Date(formDate),
      firstName: firstName.trim(),
      lastName: lastName.trim(),
      passengerFullName: `${firstName.trim()} ${lastName.trim()}`,
      travellingCountry: travellingCountry.trim(),
      passportNo: passportNo.trim(),
      contactNo: contactNo.trim(),
      isWhatsAppSame: Boolean(isWhatsAppSame),
      whatsAppNo: isWhatsAppSame ? contactNo.trim() : (whatsAppNo ? whatsAppNo.trim() : ''),
      airlineName: airlineName.trim(),
      origin: origin.trim(),
      destination: destination.trim(),
      route: `${origin.trim()} → ${destination.trim()}`,
      airlinesPnr: airlinesPnr.trim(),
      oldDate: new Date(oldDate),
      newDate: new Date(newDate),
      reissueVendorId: reissueVendorId.trim(),
      reissueVendorName: reissueVendorName ? reissueVendorName.trim() : '',
      vendorAmount: vendorAmt,
      totalContractAmount: totalAmt,
      profit: profit,
      issuingAgentName: issuingAgentName.trim(),
      issuingAgentContact: issuingAgentContact.trim(),
      agentEmail: agentEmail ? agentEmail.trim() : '',
      reservationOfficerId: reservationOfficerId.trim(),
      reservationOfficerName: reservationOfficerName ? reservationOfficerName.trim() : '',
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert old ticket reissue
    const result = await oldTicketReissues.insertOne(reissueDoc);

    // Return created reissue
    const createdReissue = await oldTicketReissues.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Old ticket reissue created successfully",
      data: createdReissue
    });

  } catch (error) {
    console.error('Create old ticket reissue error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating old ticket reissue",
      details: error.message
    });
  }
});

// GET: Get all Old Ticket Reissues with pagination and search
app.get("/api/old-ticket-reissues", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      customerId,
      reservationOfficerId,
      reissueVendorId,
      airlineName,
      travellingCountry,
      dateFrom,
      dateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by customer ID if provided
    if (customerId) {
      filter.customerId = customerId;
    }

    // Filter by reservation officer if provided
    if (reservationOfficerId) {
      filter.reservationOfficerId = reservationOfficerId;
    }

    // Filter by vendor if provided
    if (reissueVendorId) {
      filter.reissueVendorId = reissueVendorId;
    }

    // Filter by airline if provided
    if (airlineName) {
      filter.airlineName = { $regex: airlineName, $options: 'i' };
    }

    // Filter by country if provided
    if (travellingCountry) {
      filter.travellingCountry = { $regex: travellingCountry, $options: 'i' };
    }

    // Date range filter (using formDate)
    if (dateFrom || dateTo) {
      filter.formDate = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        start.setHours(0, 0, 0, 0);
        filter.formDate.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        end.setHours(23, 59, 59, 999);
        filter.formDate.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { firstName: { $regex: searchText, $options: 'i' } },
        { lastName: { $regex: searchText, $options: 'i' } },
        { passengerFullName: { $regex: searchText, $options: 'i' } },
        { passportNo: { $regex: searchText, $options: 'i' } },
        { contactNo: { $regex: searchText, $options: 'i' } },
        { whatsAppNo: { $regex: searchText, $options: 'i' } },
        { airlinesPnr: { $regex: searchText, $options: 'i' } },
        { airlineName: { $regex: searchText, $options: 'i' } },
        { origin: { $regex: searchText, $options: 'i' } },
        { destination: { $regex: searchText, $options: 'i' } },
        { route: { $regex: searchText, $options: 'i' } },
        { travellingCountry: { $regex: searchText, $options: 'i' } },
        { reissueVendorName: { $regex: searchText, $options: 'i' } },
        { issuingAgentName: { $regex: searchText, $options: 'i' } },
        { issuingAgentContact: { $regex: searchText, $options: 'i' } },
        { agentEmail: { $regex: searchText, $options: 'i' } },
        { reservationOfficerName: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      oldTicketReissues.countDocuments(filter),
      oldTicketReissues
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get old ticket reissues error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching old ticket reissues",
      details: error.message
    });
  }
});

// GET: Get single Old Ticket Reissue by ID
app.get("/api/old-ticket-reissues/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid old ticket reissue ID"
      });
    }

    const reissue = await oldTicketReissues.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!reissue) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Old ticket reissue not found"
      });
    }

    res.json({
      success: true,
      data: reissue
    });

  } catch (error) {
    console.error('Get old ticket reissue error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching old ticket reissue",
      details: error.message
    });
  }
});

// PUT: Update Old Ticket Reissue
app.put("/api/old-ticket-reissues/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid old ticket reissue ID"
      });
    }

    // Find reissue
    const reissue = await oldTicketReissues.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!reissue) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Old ticket reissue not found"
      });
    }

    // Validate contact number if being updated
    if (updateData.contactNo) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.contactNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid contact number format"
        });
      }
    }

    // Validate agent contact if being updated
    if (updateData.issuingAgentContact) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.issuingAgentContact.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid issuing agent contact format"
        });
      }
    }

    // Validate WhatsApp number if being updated
    if (updateData.whatsAppNo && updateData.whatsAppNo.trim()) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.whatsAppNo.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid WhatsApp number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.agentEmail && updateData.agentEmail.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.agentEmail.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.customerId !== undefined) {
      update.$set.customerId = updateData.customerId || null;
    }
    if (updateData.formDate !== undefined) {
      update.$set.formDate = new Date(updateData.formDate);
    }
    if (updateData.firstName !== undefined) {
      update.$set.firstName = String(updateData.firstName).trim();
    }
    if (updateData.lastName !== undefined) {
      update.$set.lastName = String(updateData.lastName).trim();
    }
    // Update full name if first or last name changed
    if (updateData.firstName !== undefined || updateData.lastName !== undefined) {
      const newFirstName = updateData.firstName !== undefined ? String(updateData.firstName).trim() : reissue.firstName;
      const newLastName = updateData.lastName !== undefined ? String(updateData.lastName).trim() : reissue.lastName;
      update.$set.passengerFullName = `${newFirstName} ${newLastName}`;
    }
    if (updateData.travellingCountry !== undefined) {
      update.$set.travellingCountry = String(updateData.travellingCountry).trim();
    }
    if (updateData.passportNo !== undefined) {
      update.$set.passportNo = String(updateData.passportNo).trim();
    }
    if (updateData.contactNo !== undefined) {
      update.$set.contactNo = String(updateData.contactNo).trim();
    }
    if (updateData.isWhatsAppSame !== undefined) {
      update.$set.isWhatsAppSame = Boolean(updateData.isWhatsAppSame);
      // If WhatsApp is same, copy contact number
      if (updateData.isWhatsAppSame) {
        update.$set.whatsAppNo = updateData.contactNo ? String(updateData.contactNo).trim() : reissue.contactNo;
      }
    }
    if (updateData.whatsAppNo !== undefined && !updateData.isWhatsAppSame) {
      update.$set.whatsAppNo = updateData.whatsAppNo ? String(updateData.whatsAppNo).trim() : '';
    }
    if (updateData.airlineName !== undefined) {
      update.$set.airlineName = String(updateData.airlineName).trim();
    }
    if (updateData.origin !== undefined) {
      update.$set.origin = String(updateData.origin).trim();
    }
    if (updateData.destination !== undefined) {
      update.$set.destination = String(updateData.destination).trim();
    }
    // Update route if origin or destination changed
    if (updateData.origin !== undefined || updateData.destination !== undefined) {
      const newOrigin = updateData.origin !== undefined ? String(updateData.origin).trim() : reissue.origin;
      const newDestination = updateData.destination !== undefined ? String(updateData.destination).trim() : reissue.destination;
      update.$set.route = `${newOrigin} → ${newDestination}`;
    }
    if (updateData.airlinesPnr !== undefined) {
      update.$set.airlinesPnr = String(updateData.airlinesPnr).trim();
    }
    if (updateData.oldDate !== undefined) {
      update.$set.oldDate = new Date(updateData.oldDate);
    }
    if (updateData.newDate !== undefined) {
      update.$set.newDate = new Date(updateData.newDate);
    }
    if (updateData.reissueVendorId !== undefined) {
      update.$set.reissueVendorId = String(updateData.reissueVendorId).trim();
    }
    if (updateData.reissueVendorName !== undefined) {
      update.$set.reissueVendorName = updateData.reissueVendorName ? String(updateData.reissueVendorName).trim() : '';
    }
    if (updateData.vendorAmount !== undefined) {
      update.$set.vendorAmount = parseFloat(updateData.vendorAmount) || 0;
    }
    if (updateData.totalContractAmount !== undefined) {
      update.$set.totalContractAmount = parseFloat(updateData.totalContractAmount) || 0;
    }
    
    // Recalculate profit if amounts changed
    if (updateData.vendorAmount !== undefined || updateData.totalContractAmount !== undefined) {
      const vendorAmt = updateData.vendorAmount !== undefined 
        ? parseFloat(updateData.vendorAmount) || 0 
        : reissue.vendorAmount || 0;
      const totalAmt = updateData.totalContractAmount !== undefined 
        ? parseFloat(updateData.totalContractAmount) || 0 
        : reissue.totalContractAmount || 0;
      update.$set.profit = totalAmt - vendorAmt;
    }
    
    if (updateData.issuingAgentName !== undefined) {
      update.$set.issuingAgentName = String(updateData.issuingAgentName).trim();
    }
    if (updateData.issuingAgentContact !== undefined) {
      update.$set.issuingAgentContact = String(updateData.issuingAgentContact).trim();
    }
    if (updateData.agentEmail !== undefined) {
      update.$set.agentEmail = updateData.agentEmail ? String(updateData.agentEmail).trim() : '';
    }
    if (updateData.reservationOfficerId !== undefined) {
      update.$set.reservationOfficerId = String(updateData.reservationOfficerId).trim();
    }
    if (updateData.reservationOfficerName !== undefined) {
      update.$set.reservationOfficerName = updateData.reservationOfficerName ? String(updateData.reservationOfficerName).trim() : '';
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Update reissue
    const result = await oldTicketReissues.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Old ticket reissue not found"
      });
    }

    // Get updated reissue
    const updatedReissue = await oldTicketReissues.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Old ticket reissue updated successfully",
      data: updatedReissue
    });

  } catch (error) {
    console.error('Update old ticket reissue error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating old ticket reissue",
      details: error.message
    });
  }
});

// DELETE: Delete Old Ticket Reissue (Soft delete)
app.delete("/api/old-ticket-reissues/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid old ticket reissue ID"
      });
    }

    // Check if reissue exists
    const reissue = await oldTicketReissues.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!reissue) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Old ticket reissue not found"
      });
    }

    // Soft delete
    await oldTicketReissues.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Old ticket reissue deleted successfully"
    });

  } catch (error) {
    console.error('Delete old ticket reissue error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting old ticket reissue",
      details: error.message
    });
  }
});


// ==================== OTHER SERVICES ROUTES ====================

// POST: Create new Other Service
app.post("/api/other-services", async (req, res) => {
  try {
    const {
      clientId,
      clientName,
      serviceType,
      serviceDate,
      date,
      description,
      phone,
      email,
      address,
      status = 'pending',
      vendorId,
      vendorName,
      serviceFee,
      vendorCost,
      otherCost,
      totalAmount,
      assignedToId,
      assignedToName,
      deliveryDate,
      referenceNumber,
      notes
    } = req.body;

    // Validation - Required fields
    if (!clientName || !clientName.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Client name is required"
      });
    }

    if (!serviceType || !serviceType.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Service type is required"
      });
    }

    // Accept both 'serviceDate' and 'date' field names
    const finalServiceDate = serviceDate || date;
    if (!finalServiceDate) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Service date is required"
      });
    }

    if (!phone || !phone.trim()) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Phone number is required"
      });
    }

    // Validate phone number format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(phone.trim())) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid phone number format"
      });
    }

    // Validate email format if provided
    if (email && email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate status
    const validStatuses = ['pending', 'in_process', 'processing', 'completed', 'cancelled', 'on_hold'];
    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid status"
      });
    }

    // Calculate total amount and profit
    const serviceAmount = parseFloat(serviceFee) || 0;
    const vendorAmount = parseFloat(vendorCost) || 0;
    const otherAmount = parseFloat(otherCost) || 0;
    const calculatedTotal = serviceAmount + vendorAmount + otherAmount;
    const finalTotal = totalAmount !== undefined ? parseFloat(totalAmount) : calculatedTotal;
    const profit = serviceAmount - vendorAmount - otherAmount;

    // Create other service document
    const now = new Date();
    const serviceDoc = {
      clientId: clientId || null,
      clientName: clientName.trim(),
      serviceType: serviceType.trim(),
      serviceDate: new Date(finalServiceDate),
      description: description ? description.trim() : '',
      phone: phone.trim(),
      email: email ? email.trim() : '',
      address: address ? address.trim() : '',
      status: status,
      vendorId: vendorId || null,
      vendorName: vendorName ? vendorName.trim() : '',
      serviceFee: serviceAmount,
      vendorCost: vendorAmount,
      otherCost: otherAmount,
      totalAmount: finalTotal,
      profit: profit,
      assignedToId: assignedToId || null,
      assignedToName: assignedToName ? assignedToName.trim() : '',
      deliveryDate: deliveryDate ? new Date(deliveryDate) : null,
      referenceNumber: referenceNumber ? referenceNumber.trim() : '',
      notes: notes ? notes.trim() : '',
      isActive: true,
      createdAt: now,
      updatedAt: now
    };

    // Insert other service
    const result = await otherServices.insertOne(serviceDoc);

    // Return created service
    const createdService = await otherServices.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: "Other service created successfully",
      data: createdService
    });

  } catch (error) {
    console.error('Create other service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while creating other service",
      details: error.message
    });
  }
});

// GET: Get all Other Services with pagination and search
app.get("/api/other-services", async (req, res) => {
  try {
    const { 
      page = 1, 
      limit = 50, 
      q, 
      status,
      serviceType,
      clientId,
      vendorId,
      assignedToId,
      dateFrom,
      dateTo
    } = req.query;

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 200);

    // Build filter
    const filter = { isActive: { $ne: false } };

    // Filter by status if provided
    if (status) {
      filter.status = status;
    }

    // Filter by service type if provided
    if (serviceType) {
      filter.serviceType = { $regex: serviceType, $options: 'i' };
    }

    // Filter by client ID if provided
    if (clientId) {
      filter.clientId = clientId;
    }

    // Filter by vendor ID if provided
    if (vendorId) {
      filter.vendorId = vendorId;
    }

    // Filter by assigned person if provided
    if (assignedToId) {
      filter.assignedToId = assignedToId;
    }

    // Date range filter
    if (dateFrom || dateTo) {
      filter.serviceDate = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        start.setHours(0, 0, 0, 0);
        filter.serviceDate.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        end.setHours(23, 59, 59, 999);
        filter.serviceDate.$lte = end;
      }
    }

    // Search filter
    if (q && String(q).trim()) {
      const searchText = String(q).trim();
      filter.$or = [
        { clientName: { $regex: searchText, $options: 'i' } },
        { serviceType: { $regex: searchText, $options: 'i' } },
        { description: { $regex: searchText, $options: 'i' } },
        { phone: { $regex: searchText, $options: 'i' } },
        { email: { $regex: searchText, $options: 'i' } },
        { referenceNumber: { $regex: searchText, $options: 'i' } },
        { vendorName: { $regex: searchText, $options: 'i' } },
        { assignedToName: { $regex: searchText, $options: 'i' } },
        { address: { $regex: searchText, $options: 'i' } }
      ];
    }

    // Get total count and data
    const [total, data] = await Promise.all([
      otherServices.countDocuments(filter),
      otherServices
        .find(filter)
        .sort({ createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum)
        .toArray()
    ]);

    res.json({
      success: true,
      services: data,
      data: data,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get other services error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching other services",
      details: error.message
    });
  }
});

// GET: Get single Other Service by ID
app.get("/api/other-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid other service ID"
      });
    }

    const service = await otherServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Other service not found"
      });
    }

    res.json({
      success: true,
      service: service,
      data: service
    });

  } catch (error) {
    console.error('Get other service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while fetching other service",
      details: error.message
    });
  }
});

// PUT: Update Other Service
app.put("/api/other-services/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid other service ID"
      });
    }

    // Find service
    const service = await otherServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Other service not found"
      });
    }

    // Validate phone number if being updated
    if (updateData.phone) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(updateData.phone.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid phone number format"
        });
      }
    }

    // Validate email format if being updated
    if (updateData.email && updateData.email.trim()) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updateData.email.trim())) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid email format"
        });
      }
    }

    // Validate status if being updated
    if (updateData.status) {
      const validStatuses = ['pending', 'in_process', 'processing', 'completed', 'cancelled', 'on_hold'];
      if (!validStatuses.includes(updateData.status)) {
        return res.status(400).json({
          success: false,
          error: true,
          message: "Invalid status"
        });
      }
    }

    // Prepare update object
    const update = { $set: { updatedAt: new Date() } };

    // Update allowed fields
    if (updateData.clientId !== undefined) {
      update.$set.clientId = updateData.clientId || null;
    }
    if (updateData.clientName !== undefined) {
      update.$set.clientName = String(updateData.clientName).trim();
    }
    if (updateData.serviceType !== undefined) {
      update.$set.serviceType = String(updateData.serviceType).trim();
    }
    // Accept both 'serviceDate' and 'date' field names
    if (updateData.serviceDate !== undefined || updateData.date !== undefined) {
      const dateValue = updateData.serviceDate || updateData.date;
      update.$set.serviceDate = new Date(dateValue);
    }
    if (updateData.description !== undefined) {
      update.$set.description = updateData.description ? String(updateData.description).trim() : '';
    }
    if (updateData.phone !== undefined) {
      update.$set.phone = String(updateData.phone).trim();
    }
    if (updateData.email !== undefined) {
      update.$set.email = updateData.email ? String(updateData.email).trim() : '';
    }
    if (updateData.address !== undefined) {
      update.$set.address = updateData.address ? String(updateData.address).trim() : '';
    }
    if (updateData.status !== undefined) {
      update.$set.status = String(updateData.status);
    }
    if (updateData.vendorId !== undefined) {
      update.$set.vendorId = updateData.vendorId || null;
    }
    if (updateData.vendorName !== undefined) {
      update.$set.vendorName = updateData.vendorName ? String(updateData.vendorName).trim() : '';
    }
    if (updateData.serviceFee !== undefined) {
      update.$set.serviceFee = parseFloat(updateData.serviceFee) || 0;
    }
    if (updateData.vendorCost !== undefined) {
      update.$set.vendorCost = parseFloat(updateData.vendorCost) || 0;
    }
    if (updateData.otherCost !== undefined) {
      update.$set.otherCost = parseFloat(updateData.otherCost) || 0;
    }
    if (updateData.totalAmount !== undefined) {
      update.$set.totalAmount = parseFloat(updateData.totalAmount) || 0;
    }
    if (updateData.assignedToId !== undefined) {
      update.$set.assignedToId = updateData.assignedToId || null;
    }
    if (updateData.assignedToName !== undefined) {
      update.$set.assignedToName = updateData.assignedToName ? String(updateData.assignedToName).trim() : '';
    }
    if (updateData.deliveryDate !== undefined) {
      update.$set.deliveryDate = updateData.deliveryDate ? new Date(updateData.deliveryDate) : null;
    }
    if (updateData.referenceNumber !== undefined) {
      update.$set.referenceNumber = updateData.referenceNumber ? String(updateData.referenceNumber).trim() : '';
    }
    if (updateData.notes !== undefined) {
      update.$set.notes = updateData.notes ? String(updateData.notes).trim() : '';
    }

    // Recalculate profit and total if any cost field is updated
    if (updateData.serviceFee !== undefined || 
        updateData.vendorCost !== undefined || 
        updateData.otherCost !== undefined ||
        updateData.totalAmount !== undefined) {
      
      const serviceFee = updateData.serviceFee !== undefined 
        ? parseFloat(updateData.serviceFee) || 0 
        : service.serviceFee || 0;
      const vendorCost = updateData.vendorCost !== undefined 
        ? parseFloat(updateData.vendorCost) || 0 
        : service.vendorCost || 0;
      const otherCost = updateData.otherCost !== undefined 
        ? parseFloat(updateData.otherCost) || 0 
        : service.otherCost || 0;
      
      const calculatedTotal = serviceFee + vendorCost + otherCost;
      update.$set.totalAmount = updateData.totalAmount !== undefined 
        ? parseFloat(updateData.totalAmount) 
        : calculatedTotal;
      
      // Calculate profit
      update.$set.profit = serviceFee - vendorCost - otherCost;
    }

    // Update service
    const result = await otherServices.updateOne(
      { _id: new ObjectId(id) },
      update
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Other service not found"
      });
    }

    // Get updated service
    const updatedService = await otherServices.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: "Other service updated successfully",
      service: updatedService,
      data: updatedService
    });

  } catch (error) {
    console.error('Update other service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while updating other service",
      details: error.message
    });
  }
});

// DELETE: Delete Other Service (Soft delete)
app.delete("/api/other-services/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid other service ID"
      });
    }

    // Check if service exists
    const service = await otherServices.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!service) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Other service not found"
      });
    }

    // Soft delete
    await otherServices.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Other service deleted successfully"
    });

  } catch (error) {
    console.error('Delete other service error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Internal server error while deleting other service",
      details: error.message
    });
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

// ==================== LICENSE ROUTES ====================
// Normalize a license document for response
const normalizeLicenseDoc = (doc) => ({
  id: String(doc._id || doc.id || ""),
  ...(doc.name && { name: doc.name }),
  ...(doc.licenseNumber && { licenseNumber: doc.licenseNumber }),
  ...(doc.type && { type: doc.type }),
  ...(doc.issuer && { issuer: doc.issuer }),
  ...(doc.issueDate && { issueDate: doc.issueDate }),
  ...(doc.expiryDate && { expiryDate: doc.expiryDate }),
  ...(doc.status && { status: doc.status }),
  ...(doc.description && { description: doc.description }),
  ...(doc.notes && { notes: doc.notes }),
  ...(doc.attachments && { attachments: doc.attachments }),
  ...(doc.createdAt && { createdAt: doc.createdAt }),
  ...(doc.updatedAt && { updatedAt: doc.updatedAt }),
  // Include any other fields from the document
  ...Object.fromEntries(
    Object.entries(doc).filter(([key]) => !['_id', '__v'].includes(key) && !['id', 'name', 'licenseNumber', 'type', 'issuer', 'issueDate', 'expiryDate', 'status', 'description', 'notes', 'attachments', 'createdAt', 'updatedAt'].includes(key))
  )
});

// GET all licenses
app.get("/api/licenses", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await licenses.find({}).sort({ createdAt: -1 }).toArray();
    return res.json(list.map(normalizeLicenseDoc));
  } catch (err) {
    console.error("/api/licenses GET error:", err);
    return res.status(500).json({ error: true, message: "Failed to load licenses" });
  }
});

// GET single license by ID
app.get("/api/licenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid license id" });
    }
    const license = await licenses.findOne({ _id: new ObjectId(id) });
    if (!license) {
      return res.status(404).json({ error: true, message: "License not found" });
    }
    return res.json(normalizeLicenseDoc(license));
  } catch (err) {
    console.error("/api/licenses/:id GET error:", err);
    return res.status(500).json({ error: true, message: "Failed to load license" });
  }
});

// CREATE license
app.post("/api/licenses", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    
    const data = req.body || {};
    const now = new Date();
    
    // Create license document - accept any fields from request body
    const doc = {
      ...data,
      createdAt: now,
      updatedAt: now
    };
    
    const result = await licenses.insertOne(doc);
    const created = await licenses.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeLicenseDoc(created));
  } catch (err) {
    console.error("/api/licenses POST error:", err);
    return res.status(500).json({ error: true, message: "Failed to create license" });
  }
});

// UPDATE license
app.put("/api/licenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid license id" });
    }
    
    const data = req.body || {};
    const update = {
      ...data,
      updatedAt: new Date()
    };
    
    // Remove _id if present (cannot update _id)
    delete update._id;
    delete update.id;
    
    const filter = { _id: new ObjectId(id) };
    const result = await licenses.findOneAndUpdate(
      filter,
      { $set: update },
      { returnDocument: 'after' }
    );
    
    const updatedDoc = result && (result.value || result);
    if (!updatedDoc) {
      return res.status(404).json({ error: true, message: "License not found" });
    }
    
    return res.json(normalizeLicenseDoc(updatedDoc));
  } catch (err) {
    console.error("/api/licenses/:id PUT error:", err);
    return res.status(500).json({ error: true, message: "Failed to update license" });
  }
});

// DELETE license
app.delete("/api/licenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid license id" });
    }
    
    const filter = { _id: new ObjectId(id) };
    const result = await licenses.deleteOne(filter);
    
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "License not found" });
    }
    
    return res.json({ success: true });
  } catch (err) {
    console.error("/api/licenses/:id DELETE error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete license" });
  }
});



// ==================== OPERATING EXPENSE CATEGORIES (CRUD) ====================
// Valid icon keys matching frontend component
const VALID_ICON_KEYS = ["FileText", "Scale", "Megaphone", "Laptop", "CreditCard", "Package", "Receipt", "RotateCcw"];

// Normalizer for operating expense category
const normalizeOpExCategory = (doc) => ({
  id: String(doc._id || doc.id || ""),
  name: doc.name || "",
  banglaName: doc.banglaName || "",
  description: doc.description || "",
  iconKey: VALID_ICON_KEYS.includes(doc.iconKey) ? doc.iconKey : "FileText",
  color: doc.color || "",
  bgColor: doc.bgColor || "",
  iconColor: doc.iconColor || "",
  totalAmount: Number(doc.totalAmount || 0),
  lastUpdated: doc.lastUpdated || null,
  itemCount: Number(doc.itemCount || 0)
});

// GET all operating expense categories
app.get("/api/operating-expenses/categories", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await operatingExpenseCategories.find({}).toArray();
    return res.json(list.map(normalizeOpExCategory));
  } catch (err) {
    console.error("GET /api/operating-expenses/categories error:", err);
    return res.status(500).json({ error: true, message: "Failed to load operating expense categories" });
  }
});

// GET one operating expense category by id
app.get("/api/operating-expenses/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const doc = await operatingExpenseCategories.findOne({ _id: new ObjectId(id) });
    if (!doc) return res.status(404).json({ error: true, message: "Category not found" });
    return res.json(normalizeOpExCategory(doc));
  } catch (err) {
    console.error("GET /api/operating-expenses/categories/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to load category" });
  }
});

// CREATE operating expense category
app.post("/api/operating-expenses/categories", async (req, res) => {
  try {
    const {
      name,
      banglaName = "",
      description = "",
      iconKey = "FileText",
      color = "",
      bgColor = "",
      iconColor = "",
      totalAmount = 0,
      lastUpdated,
      itemCount = 0
    } = req.body || {};

    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Name is required" });
    }

    // Validate iconKey
    const validIconKey = VALID_ICON_KEYS.includes(String(iconKey || "")) ? String(iconKey) : "FileText";

    // Prevent duplicate name (case-insensitive)
    const existing = await operatingExpenseCategories.findOne({
      name: { $regex: `^${String(name).trim().replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}$`, $options: "i" }
    });
    if (existing) {
      return res.status(409).json({ error: true, message: "A category with this name already exists" });
    }

    const todayStr = new Date().toISOString().slice(0, 10);
    const doc = {
      name: String(name).trim(),
      banglaName: String(banglaName || "").trim(),
      description: String(description || "").trim(),
      iconKey: validIconKey,
      color: String(color || ""),
      bgColor: String(bgColor || ""),
      iconColor: String(iconColor || ""),
      totalAmount: Number(totalAmount || 0),
      lastUpdated: lastUpdated || todayStr,
      itemCount: Number(itemCount || 0)
    };

    const result = await operatingExpenseCategories.insertOne(doc);
    const created = await operatingExpenseCategories.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeOpExCategory(created));
  } catch (err) {
    console.error("POST /api/operating-expenses/categories error:", err);
    return res.status(500).json({ error: true, message: "Failed to create category" });
  }
});

// UPDATE operating expense category
app.put("/api/operating-expenses/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }

    const allowed = [
      "name",
      "banglaName",
      "description",
      "iconKey",
      "color",
      "bgColor",
      "iconColor",
      "totalAmount",
      "lastUpdated",
      "itemCount"
    ];
    const updates = Object.fromEntries(
      Object.entries(req.body || {}).filter(([k]) => allowed.includes(k))
    );

    if (typeof updates.name !== "undefined") {
      const newName = String(updates.name).trim();
      if (!newName) {
        return res.status(400).json({ error: true, message: "Name cannot be empty" });
      }
      // Ensure uniqueness if name changed
      const existing = await operatingExpenseCategories.findOne({
        _id: { $ne: new ObjectId(id) },
        name: { $regex: `^${newName.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&")}$`, $options: "i" }
      });
      if (existing) {
        return res.status(409).json({ error: true, message: "A category with this name already exists" });
      }
      updates.name = newName;
    }

    // Validate iconKey if provided
    if (typeof updates.iconKey !== "undefined") {
      updates.iconKey = VALID_ICON_KEYS.includes(String(updates.iconKey || "")) ? String(updates.iconKey) : "FileText";
    }

    // Trim string fields
    if (typeof updates.banglaName !== "undefined") {
      updates.banglaName = String(updates.banglaName || "").trim();
    }
    if (typeof updates.description !== "undefined") {
      updates.description = String(updates.description || "").trim();
    }

    if (typeof updates.totalAmount !== "undefined") {
      updates.totalAmount = Number(updates.totalAmount || 0);
    }
    if (typeof updates.itemCount !== "undefined") {
      updates.itemCount = Number(updates.itemCount || 0);
    }

    // Auto-update lastUpdated to today if not explicitly provided
    if (typeof updates.lastUpdated === "undefined") {
      updates.lastUpdated = new Date().toISOString().slice(0, 10);
    }

    const result = await operatingExpenseCategories.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnDocument: "after" }
    );
    const updatedDoc = result && (result.value || result);
    if (!updatedDoc) return res.status(404).json({ error: true, message: "Category not found" });
    return res.json(normalizeOpExCategory(updatedDoc));
  } catch (err) {
    console.error("PUT /api/operating-expenses/categories/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update category" });
  }
});

// DELETE operating expense category
app.delete("/api/operating-expenses/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const result = await operatingExpenseCategories.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Category not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/operating-expenses/categories/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete category" });
  }
});

// ==================== PERSONAL EXPENSE CATEGORIES (CRUD) ====================
// Normalizer for personal expense category
const normalizePersonalCategory = (doc) => ({
  id: String(doc._id || doc.id || ""),
  name: doc.name || "",
  icon: doc.icon || "DollarSign",
  description: doc.description || "",
  totalAmount: Number(doc.totalAmount || 0),
  lastUpdated: doc.lastUpdated || null,
  createdAt: doc.createdAt || null
});

// GET all personal expense categories
app.get("/api/personal-expenses/categories", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await personalExpenseCategories.find({}).sort({ name: 1 }).toArray();

    // Compute live totals from main transactions collection
    const idStrings = list.map((c) => String(c._id));
    const sums = await transactions.aggregate([
      { $match: { scope: "personal-expense", type: "expense", categoryId: { $in: idStrings } } },
      { $group: { _id: "$categoryId", total: { $sum: "$amount" }, last: { $max: "$date" } } }
    ]).toArray();
    const aggMap = Object.fromEntries(sums.map((s) => [String(s._id), { total: Number(s.total || 0), last: s.last || null }]));

    const withTotals = list.map((doc) => {
      const key = String(doc._id);
      const agg = aggMap[key] || { total: Number(doc.totalAmount || 0), last: doc.lastUpdated || null };
      const normalized = normalizePersonalCategory(doc);
      return { ...normalized, totalAmount: agg.total, lastUpdated: agg.last };
    });

    return res.json(withTotals);
  } catch (err) {
    console.error("GET /api/personal-expenses/categories error:", err);
    return res.status(500).json({ error: true, message: "Failed to load personal expense categories" });
  }
});

// GET one personal expense category by id
app.get("/api/personal-expenses/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const doc = await personalExpenseCategories.findOne({ _id: new ObjectId(id) });
    if (!doc) return res.status(404).json({ error: true, message: "Category not found" });

    // Live aggregate for this category
    const sum = await transactions.aggregate([
      { $match: { scope: "personal-expense", type: "expense", categoryId: String(doc._id) } },
      { $group: { _id: "$categoryId", total: { $sum: "$amount" }, last: { $max: "$date" } } }
    ]).toArray();
    const agg = sum && sum[0] ? { total: Number(sum[0].total || 0), last: sum[0].last || null } : { total: Number(doc.totalAmount || 0), last: doc.lastUpdated || null };

    const normalized = normalizePersonalCategory(doc);
    return res.json({ ...normalized, totalAmount: agg.total, lastUpdated: agg.last });
  } catch (err) {
    console.error("GET /api/personal-expenses/categories/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to load category" });
  }
});

// CREATE personal expense category
app.post("/api/personal-expenses/categories", async (req, res) => {
  try {
    const { name, icon = "DollarSign", description = "" } = req.body || {};
    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Name is required" });
    }

    // Prevent duplicate name (case-insensitive)
    const existing = await personalExpenseCategories.findOne(
      { name: String(name).trim() },
      { collation: { locale: "en", strength: 2 } }
    );
    if (existing) {
      return res.status(409).json({ error: true, message: "A category with this name already exists" });
    }

    const doc = {
      name: String(name).trim(),
      icon: String(icon || "DollarSign"),
      description: String(description || "").trim(),
      createdAt: new Date().toISOString()
    };

    const result = await personalExpenseCategories.insertOne(doc);
    const created = await personalExpenseCategories.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizePersonalCategory(created));
  } catch (err) {
    console.error("POST /api/personal-expenses/categories error:", err);
    if (err && err.code === 11000) {
      return res.status(409).json({ error: true, message: "A category with this name already exists" });
    }
    return res.status(500).json({ error: true, message: "Failed to create category" });
  }
});

// DELETE personal expense category by id
app.delete("/api/personal-expenses/categories/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid category id" });
    }
    const result = await personalExpenseCategories.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Category not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/personal-expenses/categories/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete category" });
  }
});




// ✅ GET: Transactions stats (aggregate by category/subcategory)
// Example: /api/transactions/stats?groupBy=category,subcategory&fromDate=2025-01-01&toDate=2025-12-31
app.get("/api/transactions/stats", async (req, res) => {
  try {
    const { groupBy = "category", fromDate, toDate, partyType, partyId } = req.query || {};

    // Build match filter
    const match = { isActive: { $ne: false } };
    if (fromDate || toDate) {
      match.date = {};
      if (fromDate) match.date.$gte = new Date(fromDate);
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) end.setHours(23, 59, 59, 999);
        match.date.$lte = end;
      }
    }
    if (partyType) match.partyType = String(partyType);
    if (partyId) match.partyId = String(partyId);

    // Determine grouping keys
    const keys = String(groupBy || "").split(",").map(k => k.trim().toLowerCase()).filter(Boolean);
    const byCategory = keys.includes("category") || keys.length === 0;
    const bySubCategory = keys.includes("subcategory");

    const groupId = {};
    if (byCategory) groupId.category = "$serviceCategory";
    if (bySubCategory) groupId.subcategory = "$subCategory"; // expects subCategory saved in transactions

    const pipeline = [
      { $match: match },
      {
        $group: {
          _id: Object.keys(groupId).length ? groupId : null,
          totalCredit: { $sum: { $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0] } },
          totalDebit: { $sum: { $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0] } }
        }
      },
      {
        $project: {
          _id: 0,
          category: "$_id.category",
          subcategory: "$_id.subcategory",
          totalCredit: { $round: ["$totalCredit", 2] },
          totalDebit: { $round: ["$totalDebit", 2] },
          netAmount: { $round: [{ $subtract: ["$totalCredit", "$totalDebit"] }, 2] }
        }
      },
      { $sort: { category: 1, subcategory: 1 } }
    ];

    const data = await transactions.aggregate(pipeline).toArray();

    // Grand totals
    const totals = data.reduce((acc, r) => {
      acc.totalCredit += Number(r.totalCredit || 0);
      acc.totalDebit += Number(r.totalDebit || 0);
      acc.netAmount += Number(r.netAmount || 0);
      return acc;
    }, { totalCredit: 0, totalDebit: 0, netAmount: 0 });
    totals.totalCredit = Number(totals.totalCredit.toFixed(2));
    totals.totalDebit = Number(totals.totalDebit.toFixed(2));
    totals.netAmount = Number(totals.netAmount.toFixed(2));

    return res.json({ success: true, data, totals, groupBy: keys.length ? keys : ["category"], period: { fromDate: fromDate || null, toDate: toDate || null } });
  } catch (err) {
    console.error("/api/transactions/stats GET error:", err);
    return res.status(500).json({ error: true, message: "Failed to load transactions stats" });
  }
});






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
      passport,
      logo
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
      logo: logo || "",
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

// ✅ POST: Bulk add vendors
app.post("/vendors/bulk", async (req, res) => {
  try {
    // Support both: body = [...]  or  body = { vendors: [...] }
    const vendorsPayload = Array.isArray(req.body) ? req.body : req.body?.vendors;

    if (!Array.isArray(vendorsPayload) || vendorsPayload.length === 0) {
      return res.status(400).json({
        error: true,
        message: "vendors array is required and should not be empty",
      });
    }

    const docs = [];

    for (let i = 0; i < vendorsPayload.length; i++) {
      const item = vendorsPayload[i] || {};
      const {
        tradeName,
        tradeLocation,
        ownerName,
        contactNo,
        dob,
        nid,
        passport,
        logo,
      } = item;

      // Basic required validations (same as single add)
      if (!tradeName || !tradeLocation || !ownerName || !contactNo) {
        return res.status(400).json({
          error: true,
          message: `Row ${i + 1}: Trade Name, Location, Owner Name & Contact No are required`,
        });
      }

      const vendorId = await generateVendorId(db);
      const now = new Date();

      docs.push({
        vendorId,
        tradeName: String(tradeName).trim(),
        tradeLocation: String(tradeLocation).trim(),
        ownerName: String(ownerName).trim(),
        contactNo: String(contactNo).trim(),
        dob: dob || null,
        nid: nid ? String(nid).trim() : "",
        passport: passport ? String(passport).trim() : "",
        logo: logo || "",
        isActive: true,
        createdAt: now,
        updatedAt: now,
      });
    }

    const result = await vendors.insertMany(docs);
    const insertedIds = result.insertedIds || {};

    const responseData = docs.map((doc, index) => ({
      _id: insertedIds[index] || null,
      ...doc,
    }));

    return res.status(201).json({
      success: true,
      message: "Vendors added successfully",
      count: responseData.length,
      data: responseData,
    });
  } catch (error) {
    console.error("Error adding vendors in bulk:", error);
    res.status(500).json({
      error: true,
      message: "Internal server error while adding vendors in bulk",
    });
  }
});

// ✅ GET: All active vendors
app.get("/vendors", async (req, res) => {
  try {
    const allVendors = await vendors
      .find({ isActive: true })
      .limit(10000)
      .toArray();

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

    // Recent activity for this vendor (transactions + bills)
    const vendorObjectId = vendor._id;
    const vendorIdString = vendor.vendorId || vendorObjectId.toString();

    const recentTransactions = await transactions.find({
      partyType: 'vendor',
      isActive: { $ne: false },
      $or: [
        { partyId: vendorIdString },
        { partyId: vendorObjectId.toString() },
        { partyId: vendorObjectId }
      ]
    })
      .sort({ createdAt: -1, date: -1 })
      .limit(10)
      .toArray();

    const recentVendorBills = await vendorBills.find({
      isActive: { $ne: false },
      $or: [
        { vendorId: vendorIdString },
        { vendorId: vendorObjectId.toString() },
        { vendorId: vendorObjectId },
        { vendorName: vendor.tradeName }
      ]
    })
      .sort({ createdAt: -1, billDate: -1 })
      .limit(5)
      .toArray();

    const recentActivity = {
      transactions: recentTransactions.map(tx => ({
        transactionId: tx.transactionId,
        transactionType: tx.transactionType,
        amount: tx.amount,
        status: tx.status,
        paymentMethod: tx.paymentMethod || tx?.paymentDetails?.method || null,
        reference: tx.reference || tx?.paymentDetails?.reference || tx.transactionId,
        createdAt: tx.createdAt || tx.date
      })),
      bills: recentVendorBills.map(bill => ({
        billId: bill._id,
        billNumber: bill.billNumber || null,
        billType: bill.billType || null,
        totalAmount: bill.totalAmount || 0,
        paidAmount: bill.amount || bill.paidAmount || 0,
        paymentStatus: bill.paymentStatus || null,
        billDate: bill.billDate || null,
        createdAt: bill.createdAt || null
      }))
    };

    res.json({ success: true, vendor, recentActivity });
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
    const allowedFields = ['tradeName', 'tradeLocation', 'ownerName', 'contactNo', 'dob', 'nid', 'passport', 'logo'];
    const filteredUpdateData = {};

    // Only allow specific fields to be updated
    allowedFields.forEach(field => {
      if (updateData[field] !== undefined) {
        // Trim string fields (except logo which can be empty string)
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

// ==================== VENDOR BANK ACCOUNTS ROUTES ====================

// ✅ POST: Create vendor bank account
app.post("/vendors/:vendorId/bank-accounts", async (req, res) => {
  try {
    const { vendorId } = req.params;
    
    if (!ObjectId.isValid(vendorId)) {
      return res.status(400).json({ 
        success: false, 
        error: "Invalid vendor ID" 
      });
    }

    // Check if vendor exists
    const vendor = await vendors.findOne({ 
      _id: new ObjectId(vendorId), 
      isActive: true 
    });

    if (!vendor) {
      return res.status(404).json({ 
        success: false, 
        error: "Vendor not found" 
      });
    }

    const {
      bankName,
      accountNumber,
      accountType,
      branchName,
      accountHolder,
      accountTitle,
      initialBalance = 0,
      currency = 'BDT',
      contactNumber,
      isPrimary = false,
      notes
    } = req.body;

    // Validation
    if (!bankName || !accountNumber || !accountHolder) {
      return res.status(400).json({
        success: false,
        error: "Bank Name, Account Number, and Account Holder are required"
      });
    }

    // Check if account number already exists for this vendor
    const existingAccount = await vendorBankAccounts.findOne({
      vendorId: new ObjectId(vendorId),
      accountNumber: accountNumber.trim(),
      isDeleted: { $ne: true }
    });

    if (existingAccount) {
      return res.status(400).json({
        success: false,
        error: "Account with this number already exists for this vendor"
      });
    }

    // If setting as primary, unset other primary accounts for this vendor
    if (isPrimary) {
      await vendorBankAccounts.updateMany(
        { 
          vendorId: new ObjectId(vendorId), 
          isDeleted: { $ne: true } 
        },
        { 
          $set: { isPrimary: false, updatedAt: new Date() } 
        }
      );
    }

    const numericInitialBalance = Number(parseFloat(initialBalance) || 0);

    const bankAccountDoc = {
      vendorId: new ObjectId(vendorId),
      bankName: bankName.trim(),
      accountNumber: accountNumber.trim(),
      accountType: accountType || 'Savings',
      branchName: branchName?.trim() || '',
      accountHolder: accountHolder.trim(),
      accountTitle: accountTitle?.trim() || accountHolder.trim(),
      initialBalance: numericInitialBalance,
      currentBalance: numericInitialBalance,
      currency: currency || 'BDT',
      contactNumber: contactNumber?.trim() || null,
      isPrimary: Boolean(isPrimary),
      notes: notes?.trim() || '',
      status: 'Active',
      isDeleted: false,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await vendorBankAccounts.insertOne(bankAccountDoc);

    res.status(201).json({
      success: true,
      message: "Vendor bank account created successfully",
      data: { _id: result.insertedId, ...bankAccountDoc }
    });
  } catch (error) {
    console.error("Error creating vendor bank account:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error while creating vendor bank account"
    });
  }
});

// ✅ GET: Get all bank accounts for a vendor
app.get("/vendors/:vendorId/bank-accounts", async (req, res) => {
  try {
    const { vendorId } = req.params;

    if (!ObjectId.isValid(vendorId)) {
      return res.status(400).json({ 
        success: false, 
        error: "Invalid vendor ID" 
      });
    }

    // Check if vendor exists
    const vendor = await vendors.findOne({ 
      _id: new ObjectId(vendorId), 
      isActive: true 
    });

    if (!vendor) {
      return res.status(404).json({ 
        success: false, 
        error: "Vendor not found" 
      });
    }

    const bankAccounts = await vendorBankAccounts
      .find({ 
        vendorId: new ObjectId(vendorId), 
        isDeleted: { $ne: true } 
      })
      .sort({ isPrimary: -1, createdAt: -1 })
      .toArray();

    res.json({
      success: true,
      data: bankAccounts,
      count: bankAccounts.length
    });
  } catch (error) {
    console.error("Error fetching vendor bank accounts:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error while fetching vendor bank accounts"
    });
  }
});

// ✅ GET: Get single vendor bank account
app.get("/vendors/:vendorId/bank-accounts/:accountId", async (req, res) => {
  try {
    const { vendorId, accountId } = req.params;

    if (!ObjectId.isValid(vendorId) || !ObjectId.isValid(accountId)) {
      return res.status(400).json({ 
        success: false, 
        error: "Invalid vendor ID or account ID" 
      });
    }

    const bankAccount = await vendorBankAccounts.findOne({
      _id: new ObjectId(accountId),
      vendorId: new ObjectId(vendorId),
      isDeleted: { $ne: true }
    });

    if (!bankAccount) {
      return res.status(404).json({ 
        success: false, 
        error: "Vendor bank account not found" 
      });
    }

    res.json({
      success: true,
      data: bankAccount
    });
  } catch (error) {
    console.error("Error fetching vendor bank account:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error while fetching vendor bank account"
    });
  }
});

// ✅ PATCH: Update vendor bank account
app.patch("/vendors/:vendorId/bank-accounts/:accountId", async (req, res) => {
  try {
    const { vendorId, accountId } = req.params;

    if (!ObjectId.isValid(vendorId) || !ObjectId.isValid(accountId)) {
      return res.status(400).json({ 
        success: false, 
        error: "Invalid vendor ID or account ID" 
      });
    }

    // Check if bank account exists and belongs to vendor
    const existingAccount = await vendorBankAccounts.findOne({
      _id: new ObjectId(accountId),
      vendorId: new ObjectId(vendorId),
      isDeleted: { $ne: true }
    });

    if (!existingAccount) {
      return res.status(404).json({ 
        success: false, 
        error: "Vendor bank account not found" 
      });
    }

    const updateData = req.body;
    const allowedFields = [
      'bankName',
      'accountNumber',
      'accountType',
      'branchName',
      'accountHolder',
      'accountTitle',
      'currency',
      'contactNumber',
      'isPrimary',
      'notes',
      'status'
    ];

    const filteredUpdate = {};
    
    allowedFields.forEach(field => {
      if (updateData[field] !== undefined) {
        if (field === 'accountNumber' || field === 'bankName' || field === 'accountHolder' || field === 'accountTitle' || field === 'branchName' || field === 'contactNumber' || field === 'notes') {
          filteredUpdate[field] = String(updateData[field]).trim();
        } else if (field === 'isPrimary') {
          filteredUpdate[field] = Boolean(updateData[field]);
        } else {
          filteredUpdate[field] = updateData[field];
        }
      }
    });

    // If updating account number, check for duplicates
    if (filteredUpdate.accountNumber && filteredUpdate.accountNumber !== existingAccount.accountNumber) {
      const duplicateAccount = await vendorBankAccounts.findOne({
        vendorId: new ObjectId(vendorId),
        accountNumber: filteredUpdate.accountNumber,
        _id: { $ne: new ObjectId(accountId) },
        isDeleted: { $ne: true }
      });

      if (duplicateAccount) {
        return res.status(400).json({
          success: false,
          error: "Account with this number already exists for this vendor"
        });
      }
    }

    // If setting as primary, unset other primary accounts for this vendor
    if (filteredUpdate.isPrimary === true) {
      await vendorBankAccounts.updateMany(
        { 
          vendorId: new ObjectId(vendorId),
          _id: { $ne: new ObjectId(accountId) },
          isDeleted: { $ne: true } 
        },
        { 
          $set: { isPrimary: false, updatedAt: new Date() } 
        }
      );
    }

    filteredUpdate.updatedAt = new Date();

    const result = await vendorBankAccounts.updateOne(
      { _id: new ObjectId(accountId) },
      { $set: filteredUpdate }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: "Vendor bank account not found"
      });
    }

    const updatedAccount = await vendorBankAccounts.findOne({
      _id: new ObjectId(accountId)
    });

    res.json({
      success: true,
      message: "Vendor bank account updated successfully",
      data: updatedAccount
    });
  } catch (error) {
    console.error("Error updating vendor bank account:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error while updating vendor bank account"
    });
  }
});

// ✅ DELETE: Delete vendor bank account (soft delete)
app.delete("/vendors/:vendorId/bank-accounts/:accountId", async (req, res) => {
  try {
    const { vendorId, accountId } = req.params;

    if (!ObjectId.isValid(vendorId) || !ObjectId.isValid(accountId)) {
      return res.status(400).json({ 
        success: false, 
        error: "Invalid vendor ID or account ID" 
      });
    }

    // Check if bank account exists and belongs to vendor
    const existingAccount = await vendorBankAccounts.findOne({
      _id: new ObjectId(accountId),
      vendorId: new ObjectId(vendorId),
      isDeleted: { $ne: true }
    });

    if (!existingAccount) {
      return res.status(404).json({ 
        success: false, 
        error: "Vendor bank account not found" 
      });
    }

    // Soft delete
    const result = await vendorBankAccounts.updateOne(
      { _id: new ObjectId(accountId) },
      { 
        $set: { 
          isDeleted: true,
          status: 'Inactive',
          updatedAt: new Date()
        } 
      }
    );

    if (result.modifiedCount === 0) {
      return res.status(404).json({
        success: false,
        error: "Vendor bank account not found"
      });
    }

    res.json({
      success: true,
      message: "Vendor bank account deleted successfully"
    });
  } catch (error) {
    console.error("Error deleting vendor bank account:", error);
    res.status(500).json({
      success: false,
      error: "Internal server error while deleting vendor bank account"
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

// Helper: Generate unique Invoice ID
const generateInvoiceId = async (db) => {
  const counterCollection = db.collection("counters");
  
  // Create counter key for invoice
  const counterKey = `invoice`;
  
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
  
  // Format: INV + 00001 (e.g., INV00001)
  const serial = String(newSequence).padStart(5, '0');
  
  return `INV${serial}`;
};

// ==================== INVOICE ROUTES ====================

// ✅ POST: Create new invoice
app.post("/api/invoices", async (req, res) => {
  let session = null;
  
  try {
    const {
      date,
      customerId,
      customer,
      customerPhone,
      serviceId,
      bookingId,
      vendorId,
      vendorName,
      
      // Common billing fields
      bill,
      commission,
      discount,
      paid,
      dueCommitmentDate,
      
      // Air Ticket specific fields
      baseFare,
      tax,
      sellerDetails,
      gdsPnr,
      airlinePnr,
      ticketNo,
      passengerType,
      airlineName,
      
      // Flight Details
      flightType,
      origin,
      destination,
      flightDate,
      originOutbound,
      destinationOutbound,
      outboundFlightDate,
      originInbound,
      destinationInbound,
      inboundFlightDate,
      
      // Multi City segments
      originSegment1,
      destinationSegment1,
      flightDateSegment1,
      originSegment2,
      destinationSegment2,
      flightDateSegment2,
      
      // Customer Fare fields
      customerBaseFare,
      customerTax,
      customerCommission,
      ait,
      serviceCharge,
      
      // Vendor Fare fields
      vendorBaseFare,
      vendorTax,
      vendorCommission,
      vendorAit,
      vendorServiceCharge,
      
      // Additional fields
      branchId,
      createdBy,
      notes
    } = req.body;

    // Validation
    if (!customerId) {
      return res.status(400).json({
        success: false,
        message: 'Customer ID is required'
      });
    }

    if (!serviceId) {
      return res.status(400).json({
        success: false,
        message: 'Service ID is required'
      });
    }

    // Validate customer exists
    const customerDoc = await customers.findOne({
      $or: [
        { customerId: customerId },
        { _id: ObjectId.isValid(customerId) ? new ObjectId(customerId) : null }
      ],
      isActive: { $ne: false }
    });

    if (!customerDoc) {
      return res.status(404).json({
        success: false,
        message: 'Customer not found'
      });
    }

    // Validate vendor if provided
    let vendorDoc = null;
    if (vendorId) {
      vendorDoc = await vendors.findOne({
        $or: [
          { _id: ObjectId.isValid(vendorId) ? new ObjectId(vendorId) : null },
          { id: vendorId }
        ],
        isActive: { $ne: false }
      });

      if (!vendorDoc) {
        return res.status(404).json({
          success: false,
          message: 'Vendor not found'
        });
      }
    }

    // Start transaction session
    session = client.startSession();
    session.startTransaction();

    // Generate invoice ID
    const invoiceId = await generateInvoiceId(db);

    // Calculate totals
    // For Air Ticket: bill = baseFare + tax
    let calculatedBill = 0;
    if (baseFare !== undefined || tax !== undefined) {
      const bf = Number(baseFare) || 0;
      const tx = Number(tax) || 0;
      calculatedBill = bf + tx;
    } else {
      calculatedBill = Number(bill) || 0;
    }

    const numericCommission = Number(commission) || 0;
    const numericDiscount = Number(discount) || 0;
    const total = Math.max(0, calculatedBill + numericCommission - numericDiscount);
    const numericPaid = Number(paid) || 0;
    const due = Math.max(0, total - numericPaid);

    // Calculate customer total fare
    const custBaseFare = Number(customerBaseFare) || 0;
    const custTax = Number(customerTax) || 0;
    const custCommission = Number(customerCommission) || 0;
    const custAit = Number(ait) || 0;
    const custServiceCharge = Number(serviceCharge) || 0;
    const customerSubtotal = custBaseFare + custTax - custCommission;
    const customerTotalFare = Math.max(0, customerSubtotal + custAit + custServiceCharge);

    // Calculate vendor total fare
    const vendBaseFare = Number(vendorBaseFare) || 0;
    const vendTax = Number(vendorTax) || 0;
    const vendCommission = Number(vendorCommission) || 0;
    const vendAit = Number(vendorAit) || 0;
    const vendServiceCharge = Number(vendorServiceCharge) || 0;
    const vendorSubtotal = vendBaseFare + vendTax - vendCommission;
    const vendorTotalFare = Math.max(0, vendorSubtotal + vendAit + vendServiceCharge);

    // Build flight details object
    const flightDetails = {
      flightType: flightType || 'oneway',
      oneway: flightType === 'oneway' ? {
        origin: origin || '',
        destination: destination || '',
        flightDate: flightDate || ''
      } : null,
      roundTrip: flightType === 'round' ? {
        outbound: {
          origin: originOutbound || '',
          destination: destinationOutbound || '',
          flightDate: outboundFlightDate || ''
        },
        inbound: {
          origin: originInbound || '',
          destination: destinationInbound || '',
          flightDate: inboundFlightDate || ''
        }
      } : null,
      multiCity: flightType === 'multicity' ? {
        segment1: {
          origin: originSegment1 || '',
          destination: destinationSegment1 || '',
          flightDate: flightDateSegment1 || ''
        },
        segment2: {
          origin: originSegment2 || '',
          destination: destinationSegment2 || '',
          flightDate: flightDateSegment2 || ''
        }
      } : null
    };

    // Create invoice document
    const invoiceData = {
      invoiceId,
      date: date || new Date().toISOString().split('T')[0],
      
      // Customer information
      customerId: customerDoc.customerId || customerId,
      customerName: customer || customerDoc.name || '',
      customerPhone: customerPhone || customerDoc.mobile || '',
      customer: {
        id: customerDoc._id?.toString() || customerId,
        customerId: customerDoc.customerId,
        name: customerDoc.name,
        mobile: customerDoc.mobile,
        email: customerDoc.email || null
      },
      
      // Service information
      serviceId: serviceId,
      serviceType: serviceId, // Can be mapped to service name if needed
      
      // Booking information
      bookingId: bookingId || null,
      
      // Air Ticket specific fields
      airlineName: airlineName || null,
      gdsPnr: gdsPnr || null,
      airlinePnr: airlinePnr || null,
      ticketNo: ticketNo || null,
      passengerType: passengerType || 'adult',
      sellerDetails: sellerDetails || null,
      
      // Flight details
      flightDetails: flightDetails,
      
      // Vendor information (if provided)
      vendor: vendorDoc ? {
        id: vendorDoc._id?.toString() || vendorId,
        vendorId: vendorDoc.id || vendorId,
        tradeName: vendorDoc.tradeName || vendorName || '',
        ownerName: vendorDoc.ownerName || '',
        contactNo: vendorDoc.contactNo || ''
      } : null,
      
      // Billing information
      bill: calculatedBill,
      baseFare: baseFare ? Number(baseFare) : null,
      tax: tax ? Number(tax) : null,
      commission: numericCommission,
      discount: numericDiscount,
      total: total,
      paid: numericPaid,
      due: due,
      dueCommitmentDate: dueCommitmentDate || null,
      
      // Customer Fare breakdown
      customerFare: {
        baseFare: custBaseFare,
        tax: custTax,
        commission: custCommission,
        ait: custAit,
        serviceCharge: custServiceCharge,
        subtotal: customerSubtotal,
        total: customerTotalFare
      },
      
      // Vendor Fare breakdown
      vendorFare: vendorDoc ? {
        baseFare: vendBaseFare,
        tax: vendTax,
        commission: vendCommission,
        ait: vendAit,
        serviceCharge: vendServiceCharge,
        subtotal: vendorSubtotal,
        total: vendorTotalFare
      } : null,
      
      // Profit calculation (if vendor fare exists)
      profit: vendorDoc ? (customerTotalFare - vendorTotalFare) : null,
      
      // Additional fields
      branchId: branchId || 'main',
      createdBy: createdBy || 'SYSTEM',
      notes: notes || '',
      status: due > 0 ? 'pending' : 'paid',
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Insert invoice
    const invoiceResult = await invoices.insertOne(invoiceData, { session });

    // Update customer's total amount and due if needed
    const customerTotalAmount = (customerDoc.totalAmount || 0) + total;
    const customerPaidAmount = (customerDoc.paidAmount || 0) + numericPaid;
    const customerDue = customerTotalAmount - customerPaidAmount;

    await customers.updateOne(
      { _id: customerDoc._id },
      {
        $set: {
          totalAmount: customerTotalAmount,
          paidAmount: customerPaidAmount,
          updatedAt: new Date()
        }
      },
      { session }
    );

    // Commit transaction
    await session.commitTransaction();

    // Fetch created invoice
    const createdInvoice = await invoices.findOne({ _id: invoiceResult.insertedId });

    res.status(201).json({
      success: true,
      message: 'Invoice created successfully',
      invoice: createdInvoice
    });

  } catch (error) {
    // Rollback on error
    if (session && session.inTransaction()) {
      await session.abortTransaction();
    }
    
    console.error('Create invoice error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create invoice',
      error: error.message
    });
  } finally {
    // End session
    if (session) {
      session.endSession();
    }
  }
});

// ✅ GET: Get all invoices with filters and pagination
app.get("/api/invoices", async (req, res) => {
  try {
    const {
      customerId,
      serviceId,
      status,
      fromDate,
      toDate,
      page = 1,
      limit = 20,
      q
    } = req.query || {};

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);
    const skip = (pageNum - 1) * limitNum;

    // Build query
    const query = { isActive: { $ne: false } };

    if (customerId) {
      query.$or = [
        { customerId: customerId },
        { 'customer.id': customerId },
        { 'customer.customerId': customerId }
      ];
    }

    if (serviceId) {
      query.serviceId = serviceId;
    }

    if (status) {
      query.status = status;
    }

    if (fromDate || toDate) {
      query.date = {};
      if (fromDate) query.date.$gte = fromDate;
      if (toDate) query.date.$lte = toDate;
    }

    if (q) {
      const searchTerm = String(q).trim();
      const searchConditions = [
        { invoiceId: { $regex: searchTerm, $options: 'i' } },
        { bookingId: { $regex: searchTerm, $options: 'i' } },
        { customerName: { $regex: searchTerm, $options: 'i' } },
        { customerPhone: { $regex: searchTerm, $options: 'i' } },
        { airlinePnr: { $regex: searchTerm, $options: 'i' } },
        { gdsPnr: { $regex: searchTerm, $options: 'i' } },
        { ticketNo: { $regex: searchTerm, $options: 'i' } }
      ];
      
      // If customerId filter exists, combine with AND
      if (query.$or) {
        query.$and = [
          { $or: query.$or },
          { $or: searchConditions }
        ];
        delete query.$or;
      } else {
        query.$or = searchConditions;
      }
    }

    // Get total count
    const total = await invoices.countDocuments(query);

    // Get invoices
    const invoicesList = await invoices
      .find(query)
      .sort({ createdAt: -1, date: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data: invoicesList,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get invoices error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch invoices',
      error: error.message
    });
  }
});

// ✅ GET: Get single invoice by ID
app.get("/api/invoices/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { invoiceId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const invoice = await invoices.findOne(query);

    if (!invoice) {
      return res.status(404).json({
        success: false,
        message: 'Invoice not found'
      });
    }

    res.json({
      success: true,
      invoice
    });

  } catch (error) {
    console.error('Get invoice error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch invoice',
      error: error.message
    });
  }
});

// ✅ PUT: Update invoice by ID
app.put("/api/invoices/:id", async (req, res) => {
  let session = null;
  
  try {
    const { id } = req.params;
    const updateData = req.body;

    // Find invoice
    const query = {
      $or: [
        { invoiceId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const existingInvoice = await invoices.findOne(query);

    if (!existingInvoice) {
      return res.status(404).json({
        success: false,
        message: 'Invoice not found'
      });
    }

    // Start transaction
    session = client.startSession();
    session.startTransaction();

    // Build update object (only update provided fields)
    const updateFields = {
      updatedAt: new Date()
    };

    // Update allowed fields
    const allowedFields = [
      'date', 'bookingId', 'airlineName', 'gdsPnr', 'airlinePnr', 
      'ticketNo', 'passengerType', 'flightDetails', 'bill', 'baseFare', 
      'tax', 'commission', 'discount', 'paid', 'dueCommitmentDate',
      'customerFare', 'vendorFare', 'notes', 'status'
    ];

    for (const field of allowedFields) {
      if (updateData[field] !== undefined) {
        updateFields[field] = updateData[field];
      }
    }

    // Recalculate totals if billing fields changed
    if (updateData.bill !== undefined || updateData.commission !== undefined || 
        updateData.discount !== undefined || updateData.paid !== undefined) {
      const bill = updateData.bill !== undefined ? Number(updateData.bill) : (existingInvoice.bill || 0);
      const commission = updateData.commission !== undefined ? Number(updateData.commission) : (existingInvoice.commission || 0);
      const discount = updateData.discount !== undefined ? Number(updateData.discount) : (existingInvoice.discount || 0);
      const paid = updateData.paid !== undefined ? Number(updateData.paid) : (existingInvoice.paid || 0);
      
      updateFields.total = Math.max(0, bill + commission - discount);
      updateFields.due = Math.max(0, updateFields.total - paid);
      updateFields.status = updateFields.due > 0 ? 'pending' : 'paid';
    }

    // Update invoice
    await invoices.updateOne(
      { _id: existingInvoice._id },
      { $set: updateFields },
      { session }
    );

    // Commit transaction
    await session.commitTransaction();

    // Fetch updated invoice
    const updatedInvoice = await invoices.findOne({ _id: existingInvoice._id });

    res.json({
      success: true,
      message: 'Invoice updated successfully',
      invoice: updatedInvoice
    });

  } catch (error) {
    // Rollback on error
    if (session && session.inTransaction()) {
      await session.abortTransaction();
    }
    
    console.error('Update invoice error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update invoice',
      error: error.message
    });
  } finally {
    // End session
    if (session) {
      session.endSession();
    }
  }
});

// ✅ DELETE: Delete invoice by ID (soft delete)
app.delete("/api/invoices/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { invoiceId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const invoice = await invoices.findOne(query);

    if (!invoice) {
      return res.status(404).json({
        success: false,
        message: 'Invoice not found'
      });
    }

    // Soft delete
    await invoices.updateOne(
      { _id: invoice._id },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: 'Invoice deleted successfully'
    });

  } catch (error) {
    console.error('Delete invoice error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete invoice',
      error: error.message
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
      employeeReference,
      operatingExpenseCategoryId,
      moneyExchangeInfo,
      meta: incomingMeta
    } = req.body;

    // Extract values from nested objects if provided
    const finalAmount = amount || paymentDetails?.amount;
    const finalPartyId = partyId || customerId;
    const finalTargetAccountId = targetAccountId || creditAccount?.id || debitAccount?.id;
    const finalFromAccountId = fromAccountId || debitAccount?.id;
    const finalToAccountId = toAccountId || creditAccount?.id;
    const finalServiceCategory = serviceCategory || category;
    const finalSubCategory = typeof req.body?.subCategory !== 'undefined' ? String(req.body.subCategory || '').trim() : undefined;
    const finalOperatingExpenseCategoryId = operatingExpenseCategoryId || req.body?.operatingExpenseCategory?.id;
    const meta = (incomingMeta && typeof incomingMeta === 'object') ? { ...incomingMeta } : {};
    if (meta.packageId) {
      meta.packageId = String(meta.packageId);
    }
    
    // Determine final party type defensively
    // Handle customerType from frontend (e.g., 'money-exchange') and map to partyType
    let finalPartyType = String(partyType || req.body?.customerType || '').toLowerCase();

    // 1. Validation - আগে সব validate করুন
    console.log('Transaction Payload:', JSON.stringify(req.body, null, 2)); // Debug log

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

    // Validate charge if present
    let chargeAmount = 0;
    // Check both req.body.charge and req.body.paymentDetails.charge
    const rawCharge = req.body.charge !== undefined ? req.body.charge : (paymentDetails?.charge);
    
    if (rawCharge !== undefined && rawCharge !== null) {
      chargeAmount = parseFloat(rawCharge);
      if (isNaN(chargeAmount)) {
        return res.status(400).json({
          success: false,
          message: "Charge must be a valid number"
        });
      }
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

    // Special-case detection: Miraj Industries farm incomes/expenses by numeric id
    let mirajType = null; // 'miraj-income' | 'miraj-expense'
    let mirajDoc = null;
    const numericPartyId = Number(searchPartyId);
    if (!isNaN(numericPartyId)) {
      try {
        const fi = await farmIncomes.findOne({ id: numericPartyId });
        if (fi) {
          mirajType = 'miraj-income';
          mirajDoc = fi;
        } else {
          const fe = await farmExpenses.findOne({ id: numericPartyId });
          if (fe) {
            mirajType = 'miraj-expense';
            mirajDoc = fe;
          }
        }
      } catch (_) {}
    }

    if (mirajType) {
      // Treat as virtual party; skip regular party lookups
      finalPartyType = mirajType;
      party = {
        _id: null,
        name: mirajType === 'miraj-income' ? (mirajDoc.customer || mirajDoc.source || 'Income') : (mirajDoc.vendor || 'Expense'),
        phone: null,
        email: null
      };
    } else if (finalPartyType === 'customer') {
      // First try airCustomers collection (main collection)
      const airCustomerCondition = isValidObjectId
        ? { $or: [{ customerId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: { $ne: false } }
        : { $or: [{ customerId: searchPartyId }, { _id: searchPartyId }], isActive: { $ne: false } };
      party = await airCustomers.findOne(airCustomerCondition);
      // Mark that this is an airCustomer
      if (party) {
        party._isAirCustomer = true;
      }
      // If not found in airCustomers, try otherCustomers collection (Additional Services customers)
      if (!party) {
        try {
          const otherCustomerCondition = isValidObjectId
            ? { $or: [{ customerId: searchPartyId }, { id: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: searchPartyId }, { id: searchPartyId }, { _id: searchPartyId }], isActive: { $ne: false } };
          party = await otherCustomers.findOne(otherCustomerCondition);
          if (party) {
            party._isOtherCustomer = true;
          }
        } catch (e) {
          // otherCustomers collection error, continue
          console.warn('Error searching otherCustomers:', e.message);
        }
      }
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
    } else if (finalPartyType === 'loan') {
      const loanCondition = isValidObjectId
        ? { $or: [{ loanId: searchPartyId }, { _id: new ObjectId(searchPartyId) }], isActive: { $ne: false } }
        : { $or: [{ loanId: searchPartyId }, { _id: searchPartyId }], isActive: { $ne: false } };
      party = await loans.findOne(loanCondition);
    } else if (finalPartyType === 'money-exchange' || finalPartyType === 'money_exchange') {
      // Handle money exchange party type
      const exchangeCondition = isValidObjectId
        ? { _id: new ObjectId(searchPartyId), isActive: { $ne: false } }
        : { _id: searchPartyId, isActive: { $ne: false } };
      // Also try searching by the ID from moneyExchangeInfo if provided
      if (moneyExchangeInfo && moneyExchangeInfo.id) {
        const exchangeId = moneyExchangeInfo.id;
        const exchangeIdValid = ObjectId.isValid(exchangeId);
        const exchangeCond = exchangeIdValid
          ? { _id: new ObjectId(exchangeId), isActive: { $ne: false } }
          : { _id: exchangeId, isActive: { $ne: false } };
        party = await exchanges.findOne(exchangeCond);
      } else {
        party = await exchanges.findOne(exchangeCondition);
      }
      // If party not found but moneyExchangeInfo is provided, create virtual party
      if (!party && moneyExchangeInfo) {
        party = {
          _id: moneyExchangeInfo.id ? (ObjectId.isValid(moneyExchangeInfo.id) ? new ObjectId(moneyExchangeInfo.id) : moneyExchangeInfo.id) : null,
          fullName: moneyExchangeInfo.fullName || moneyExchangeInfo.currencyName || 'Money Exchange',
          mobileNumber: moneyExchangeInfo.mobileNumber || null,
          type: moneyExchangeInfo.type || null,
          currencyCode: moneyExchangeInfo.currencyCode || null,
          currencyName: moneyExchangeInfo.currencyName || null,
          exchangeRate: moneyExchangeInfo.exchangeRate || null,
          quantity: moneyExchangeInfo.quantity || null,
          amount_bdt: moneyExchangeInfo.amount_bdt || moneyExchangeInfo.amount || null
        };
      }
    }

    // Allow transactions even if party is not found in database
    // Party information will be stored as provided
    if (!party && partyType && partyType !== 'other' && finalPartyType !== 'money-exchange' && finalPartyType !== 'money_exchange') {
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
        subCategory: finalSubCategory || null,
        partyType: finalPartyType,
        partyId: finalPartyId,
        partyName: party?.name || party?.customerName || party?.firstName || (party?.firstName && party?.lastName ? `${party.firstName} ${party.lastName}` : null) || party?.agentName || party?.tradeName || party?.vendorName || party?.fullName || party?.currencyName || 'Unknown',
        partyPhone: party?.phone || party?.customerPhone || party?.contactNo || party?.mobile || party?.mobileNumber || party?.whatsappNo || null,
        partyEmail: party?.email || party?.customerEmail || null,
        invoiceId,
        paymentMethod,
        targetAccountId: transactionType === 'transfer' ? finalToAccountId : finalTargetAccountId,
        fromAccountId: transactionType === 'transfer' ? finalFromAccountId : null,
        accountManagerId,
        // Include nested objects for compatibility
        debitAccount: debitAccount || (transactionType === 'debit' ? { id: finalTargetAccountId } : null),
        creditAccount: creditAccount || (transactionType === 'credit' ? { id: finalTargetAccountId } : null),
        paymentDetails: {
          ...(paymentDetails || {}),
          amount: numericAmount,
          charge: chargeAmount || 0
        },
        customerBankAccount: customerBankAccount || null,
        meta: Object.keys(meta || {}).length ? meta : undefined,
        // Store money exchange information if available
        moneyExchangeInfo: (finalPartyType === 'money-exchange' || finalPartyType === 'money_exchange') && moneyExchangeInfo ? {
          id: moneyExchangeInfo.id || party?._id?.toString() || null,
          fullName: moneyExchangeInfo.fullName || party?.fullName || null,
          mobileNumber: moneyExchangeInfo.mobileNumber || party?.mobileNumber || null,
          type: moneyExchangeInfo.type || party?.type || null,
          currencyCode: moneyExchangeInfo.currencyCode || party?.currencyCode || null,
          currencyName: moneyExchangeInfo.currencyName || party?.currencyName || null,
          exchangeRate: moneyExchangeInfo.exchangeRate || party?.exchangeRate || null,
          quantity: moneyExchangeInfo.quantity || party?.quantity || null,
          amount_bdt: moneyExchangeInfo.amount_bdt || moneyExchangeInfo.amount || party?.amount_bdt || null
        } : null,
        amount: numericAmount,
        charge: chargeAmount,
        totalAmount: numericAmount + chargeAmount,
        branchId: branch.branchId,
        branchName: branch.branchName,
        branchCode: branch.branchCode,
        createdBy: createdBy || 'SYSTEM',
        notes: notes || '',
        reference: reference || paymentDetails?.reference || transactionId,
        employeeReference: employeeReference || null,
        operatingExpenseCategoryId: finalOperatingExpenseCategoryId && ObjectId.isValid(String(finalOperatingExpenseCategoryId)) ? String(finalOperatingExpenseCategoryId) : null,
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
        const isAirCustomer = party._isAirCustomer === true;
        const isOtherCustomer = party._isOtherCustomer === true;

        // Determine which collection to update
        const customerCollection = isAirCustomer ? airCustomers : (isOtherCustomer ? otherCustomers : airCustomers);

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
        // For airCustomers, also update totalAmount on debit (when customer owes more)
        if (isAirCustomer && transactionType === 'debit') {
          customerUpdate.$inc.totalAmount = (customerUpdate.$inc.totalAmount || 0) + numericAmount;
        }
        await customerCollection.updateOne({ _id: party._id }, customerUpdate, { session });
        const after = await customerCollection.findOne({ _id: party._id }, { session });
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
          await customerCollection.updateOne({ _id: party._id }, { $set: setClamp }, { session });
        }
        updatedCustomer = await customerCollection.findOne({ _id: party._id }, { session });

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
            await triggerFamilyRecomputeForHaji(afterH, { session });
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
          await triggerFamilyRecomputeForHaji(afterHaji, { session });
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
          await triggerFamilyRecomputeForUmrah(afterUmrah, { session });
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

      // 8.6 If party is a loan, update loan profile amounts (totalAmount/paidAmount/totalDue)
      if (finalPartyType === 'loan' && party && party._id) {
        const isReceivingLoan = String(party.loanDirection || '').toLowerCase() === 'receiving';
        let dueDelta = 0;
        const loanUpdate = { $set: { updatedAt: new Date() }, $inc: { } };

        if (isReceivingLoan) {
          // Receiving loan perspective: credit = principal in, increases due; debit = repayment
          if (transactionType === 'credit') {
            dueDelta = numericAmount;
            loanUpdate.$inc.totalAmount = (loanUpdate.$inc.totalAmount || 0) + numericAmount;
          } else if (transactionType === 'debit') {
            dueDelta = -numericAmount;
            loanUpdate.$inc.paidAmount = (loanUpdate.$inc.paidAmount || 0) + numericAmount;
          }
        } else {
          // Giving loan perspective (default): debit = principal out, increases due; credit = repayment
          if (transactionType === 'debit') {
            dueDelta = numericAmount;
            loanUpdate.$inc.totalAmount = (loanUpdate.$inc.totalAmount || 0) + numericAmount;
          } else if (transactionType === 'credit') {
            dueDelta = -numericAmount;
            loanUpdate.$inc.paidAmount = (loanUpdate.$inc.paidAmount || 0) + numericAmount;
          }
        }

        loanUpdate.$inc.totalDue = dueDelta;
        await loans.updateOne({ _id: party._id }, loanUpdate, { session });
        const afterLoan = await loans.findOne({ _id: party._id }, { session });
        const clampLoan = {};
        if ((afterLoan.totalDue || 0) < 0) clampLoan.totalDue = 0;
        if ((afterLoan.paidAmount || 0) < 0) clampLoan.paidAmount = 0;
        if ((afterLoan.totalAmount || 0) < 0) clampLoan.totalAmount = 0;
        if (typeof afterLoan.totalAmount === 'number' && typeof afterLoan.paidAmount === 'number' && afterLoan.paidAmount > afterLoan.totalAmount) {
          clampLoan.paidAmount = afterLoan.totalAmount;
        }
        if (Object.keys(clampLoan).length) {
          clampLoan.updatedAt = new Date();
          await loans.updateOne({ _id: party._id }, { $set: clampLoan }, { session });
        }
      // Ensure loan is marked Active once any transaction is recorded against it
      await loans.updateOne({ _id: party._id, status: { $ne: 'Active' } }, { $set: { status: 'Active', updatedAt: new Date() } }, { session });
      }

      transactionResult = await transactions.insertOne(transactionData, { session });

      // 8.7 If Miraj (farm) income/expense, sync the corresponding doc's amount to transaction amount
      if (finalPartyType === 'miraj-income' && mirajDoc) {
        await farmIncomes.updateOne(
          { id: Number(finalPartyId) },
          { $set: { amount: numericAmount, updatedAt: new Date() } },
          { session }
        );
      } else if (finalPartyType === 'miraj-expense' && mirajDoc) {
        await farmExpenses.updateOne(
          { id: Number(finalPartyId) },
          { $set: { amount: numericAmount, updatedAt: new Date() } },
          { session }
        );
      }

      // 8.8 If operating expense category is provided, update category totals
      if (finalOperatingExpenseCategoryId && ObjectId.isValid(String(finalOperatingExpenseCategoryId))) {
        const todayStr = new Date().toISOString().slice(0, 10);
        // Only update for debit transactions (expenses)
        if (transactionType === 'debit') {
          await operatingExpenseCategories.updateOne(
            { _id: new ObjectId(String(finalOperatingExpenseCategoryId)) },
            { 
              $inc: { totalAmount: numericAmount, itemCount: 1 }, 
              $set: { lastUpdated: todayStr } 
            },
            { session }
          );
        }
      }

      // 8.9 If party is a money exchange, link transaction to exchange record
      if ((finalPartyType === 'money-exchange' || finalPartyType === 'money_exchange') && party && party._id) {
        const exchangeId = ObjectId.isValid(party._id) ? party._id : new ObjectId(party._id);
        // Update exchange record to link with transaction
        await exchanges.updateOne(
          { _id: exchangeId },
          { 
            $set: { 
              transactionId: transactionId,
              transactionLinked: true,
              updatedAt: new Date() 
            } 
          },
          { session }
        );
      } else if ((finalPartyType === 'money-exchange' || finalPartyType === 'money_exchange') && moneyExchangeInfo && moneyExchangeInfo.id) {
        // If party was not found but moneyExchangeInfo has ID, try to update it
        const exchangeId = ObjectId.isValid(moneyExchangeInfo.id) ? new ObjectId(moneyExchangeInfo.id) : moneyExchangeInfo.id;
        try {
          await exchanges.updateOne(
            { _id: exchangeId },
            { 
              $set: { 
                transactionId: transactionId,
                transactionLinked: true,
                updatedAt: new Date() 
              } 
            },
            { session }
          );
        } catch (exchangeUpdateErr) {
          console.warn('Failed to link exchange with transaction:', exchangeUpdateErr?.message);
          // Don't fail the transaction if exchange update fails
        }
      }

      // 8.10 If invoiceId is provided, update invoice status to 'paid' (only status update, no amount change)
      let updatedInvoice = null;
      if (invoiceId) {
        try {
          const invoiceQuery = ObjectId.isValid(invoiceId)
            ? { $or: [{ invoiceId: invoiceId }, { _id: new ObjectId(invoiceId) }], isActive: { $ne: false } }
            : { $or: [{ invoiceId: invoiceId }, { _id: invoiceId }], isActive: { $ne: false } };

          const invoiceDoc = await invoices.findOne(invoiceQuery, { session });
          
          if (invoiceDoc) {
            // Only update invoice status to 'paid', don't change total, paid, due amounts
            await invoices.updateOne(
              { _id: invoiceDoc._id },
              {
                $set: {
                  status: 'paid',
                  updatedAt: new Date()
                }
              },
              { session }
            );
            
            // Fetch updated invoice
            updatedInvoice = await invoices.findOne({ _id: invoiceDoc._id }, { session });
            
            console.log(`Invoice ${invoiceDoc.invoiceId} status updated to 'paid'`);
          } else {
            console.warn(`Invoice not found for invoiceId: ${invoiceId}`);
          }
        } catch (invoiceUpdateErr) {
          console.warn('Failed to update invoice status from transaction:', invoiceUpdateErr?.message);
          // Don't fail the transaction if invoice update fails
        }
      }

      // 8.11 If linked to an agent package, recalculate payment summary
      if (meta.packageId) {
        try {
          const packageIdStr = String(meta.packageId);
          const packageIdCandidates = [packageIdStr];
          if (ObjectId.isValid(packageIdStr)) {
            packageIdCandidates.push(new ObjectId(packageIdStr));
          }

          const summary = await transactions
            .aggregate(
              [
                {
                  $match: {
                    isActive: { $ne: false },
                    'meta.packageId': { $in: packageIdCandidates }
                  }
                },
                {
                  $group: {
                    _id: null,
                    totalCredit: {
                      $sum: {
                        $cond: [{ $eq: ['$transactionType', 'credit'] }, '$amount', 0]
                      }
                    },
                    totalDebit: {
                      $sum: {
                        $cond: [{ $eq: ['$transactionType', 'debit'] }, '$amount', 0]
                      }
                    },
                    lastPaymentDate: {
                      $max: {
                        $cond: [{ $eq: ['$transactionType', 'credit'] }, '$date', null]
                      }
                    }
                  }
                }
              ],
              { session }
            )
            .toArray();

          const totalPaid = summary?.[0]?.totalCredit || 0;
          const lastPaymentDate = summary?.[0]?.lastPaymentDate || null;
          const targetPackageId = ObjectId.isValid(packageIdStr) ? new ObjectId(packageIdStr) : packageIdStr;

          await agentPackages.updateOne(
            { _id: targetPackageId },
            {
              $set: {
                totalPaid,
                lastPaymentDate,
                updatedAt: new Date()
              }
            },
            { session }
          );
        } catch (packageSummaryErr) {
          console.warn('Failed to update agent package payment summary:', packageSummaryErr?.message);
        }
      }

      // 9. Commit transaction
      await session.commitTransaction();

      res.json({
        success: true,
        transaction: { ...transactionData, _id: transactionResult.insertedId },
        agent: updatedAgent || null,
        customer: updatedCustomer || null,
        vendor: updatedVendor || null,
        invoice: updatedInvoice || null
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
      scope,
      categoryId,
      fromDate,
      toDate,
      page = 1,
      limit = 20,
      q
    } = req.query || {};

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    // Special branch: Personal expense transactions (stored with string dates and custom fields)
    if (String(scope) === "personal-expense") {
      const peFilter = { scope: "personal-expense", type: "expense" };
      if (fromDate || toDate) {
        peFilter.date = {};
        if (fromDate) peFilter.date.$gte = String(fromDate).slice(0, 10);
        if (toDate) peFilter.date.$lte = String(toDate).slice(0, 10);
      }
      if (categoryId && ObjectId.isValid(String(categoryId))) {
        peFilter.categoryId = String(categoryId);
      }

      const cursor = transactions
        .find(peFilter)
        .sort({ date: -1, createdAt: -1 })
        .skip((pageNum - 1) * limitNum)
        .limit(limitNum);

      const [items, total] = await Promise.all([
        cursor.toArray(),
        transactions.countDocuments(peFilter)
      ]);

      const data = items.map((doc) => ({
        id: String(doc._id || doc.id || ""),
        date: doc.date || new Date().toISOString().slice(0, 10),
        amount: Number(doc.amount || 0),
        categoryId: String(doc.categoryId || ""),
        categoryName: String(doc.categoryName || ""),
        description: String(doc.description || ""),
        tags: Array.isArray(doc.tags) ? doc.tags : [],
        createdAt: doc.createdAt || null
      }));

      return res.json({
        success: true,
        data,
        pagination: {
          page: pageNum,
          limit: limitNum,
          total,
          totalPages: Math.ceil(total / limitNum)
        }
      });
    }

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

    // Calculate total charge for the filtered transactions
    const summary = await transactions.aggregate([
      { $match: filter },
      { $group: { _id: null, totalCharge: { $sum: "$charge" }, totalAmount: { $sum: "$amount" } } }
    ]).toArray();

    const totalCharge = summary[0]?.totalCharge || 0;
    const totalAmount = summary[0]?.totalAmount || 0;

    res.json({
      success: true,
      data: items,
      summary: {
        totalCharge,
        totalAmount
      },
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

// ✅ DELETE: Delete transaction and reverse all related operations
app.delete("/api/transactions/:id", async (req, res) => {
  let session = null;

  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ success: false, message: "Invalid transaction ID" });
    }

    // Find the transaction
    const tx = await transactions.findOne({ 
      _id: new ObjectId(id), 
      isActive: { $ne: false },
      scope: { $ne: "personal-expense" } // Exclude personal-expense transactions (they have their own endpoint)
    });

    if (!tx) {
      return res.status(404).json({ success: false, message: "Transaction not found" });
    }

    // Start MongoDB session for atomic operations
    session = db.client.startSession();
    session.startTransaction();

    try {
      const numericAmount = Number(tx.amount || 0);
      const transactionType = tx.transactionType; // credit | debit | transfer
      const partyType = tx.partyType;
      const serviceCategory = tx.serviceCategory || '';
      const categoryText = String(serviceCategory).toLowerCase();
      const isHajjCategory = categoryText.includes('haj');
      const isUmrahCategory = categoryText.includes('umrah');

      // 1. Reverse bank account balance changes
      if (transactionType === "credit" && tx.targetAccountId) {
        const account = await bankAccounts.findOne({ _id: new ObjectId(tx.targetAccountId) }, { session });
        if (account) {
          const newBalance = (account.currentBalance || 0) - numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(tx.targetAccountId) },
            {
              $set: { currentBalance: newBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: -numericAmount,
                  type: 'reversal',
                  note: `Transaction deletion: ${tx.transactionId || id}`,
                  at: new Date()
                }
              }
            },
            { session }
          );
        }
      } else if (transactionType === "debit" && tx.targetAccountId) {
        const account = await bankAccounts.findOne({ _id: new ObjectId(tx.targetAccountId) }, { session });
        if (account) {
          const newBalance = (account.currentBalance || 0) + numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(tx.targetAccountId) },
            {
              $set: { currentBalance: newBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: numericAmount,
                  type: 'reversal',
                  note: `Transaction deletion: ${tx.transactionId || id}`,
                  at: new Date()
                }
              }
            },
            { session }
          );
        }
      } else if (transactionType === "transfer" && tx.fromAccountId && tx.targetAccountId) {
        const fromAccount = await bankAccounts.findOne({ _id: new ObjectId(tx.fromAccountId) }, { session });
        const toAccount = await bankAccounts.findOne({ _id: new ObjectId(tx.targetAccountId) }, { session });
        
        if (fromAccount) {
          const fromNewBalance = (fromAccount.currentBalance || 0) + numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(tx.fromAccountId) },
            {
              $set: { currentBalance: fromNewBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: numericAmount,
                  type: 'reversal',
                  note: `Transaction deletion: ${tx.transactionId || id}`,
                  at: new Date()
                }
              }
            },
            { session }
          );
        }
        
        if (toAccount) {
          const toNewBalance = (toAccount.currentBalance || 0) - numericAmount;
          await bankAccounts.updateOne(
            { _id: new ObjectId(tx.targetAccountId) },
            {
              $set: { currentBalance: toNewBalance, updatedAt: new Date() },
              $push: {
                balanceHistory: {
                  amount: -numericAmount,
                  type: 'reversal',
                  note: `Transaction deletion: ${tx.transactionId || id}`,
                  at: new Date()
                }
              }
            },
            { session }
          );
        }
      }

      // 2. Reverse party due/paid amount changes
      if (tx.partyId && tx.partyType) {
        const partyId = tx.partyId;
        const isValidObjectId = ObjectId.isValid(partyId);

        // Reverse dueDelta: opposite of creation
        // Creation: debit => +amount, credit => -amount
        // Deletion: debit => -amount, credit => +amount
        const dueDelta = transactionType === 'debit' ? -numericAmount : (transactionType === 'credit' ? numericAmount : 0);

        // 2.1 Agent
        if (partyType === 'agent') {
          const agentCond = isValidObjectId
            ? { $or: [{ agentId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ agentId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const agent = await agents.findOne(agentCond, { session });
          if (agent) {
            const agentUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
            if (isHajjCategory) {
              agentUpdate.$inc.hajDue = (agentUpdate.$inc.hajDue || 0) + dueDelta;
            }
            if (isUmrahCategory) {
              agentUpdate.$inc.umrahDue = (agentUpdate.$inc.umrahDue || 0) + dueDelta;
            }
            if (transactionType === 'credit') {
              agentUpdate.$inc.totalDeposit = (agentUpdate.$inc.totalDeposit || 0) - numericAmount;
            }
            await agents.updateOne({ _id: agent._id }, agentUpdate, { session });
            
            // Clamp negatives
            const after = await agents.findOne({ _id: agent._id }, { session });
            const setClamp = {};
            if ((after.totalDue || 0) < 0) setClamp.totalDue = 0;
            if ((after.hajDue !== undefined) && after.hajDue < 0) setClamp.hajDue = 0;
            if ((after.umrahDue !== undefined) && after.umrahDue < 0) setClamp.umrahDue = 0;
            if (Object.keys(setClamp).length) {
              setClamp.updatedAt = new Date();
              await agents.updateOne({ _id: agent._id }, { $set: setClamp }, { session });
            }
          }
        }

        // 2.2 Vendor
        if (partyType === 'vendor') {
          const vendorCond = isValidObjectId
            ? { $or: [{ vendorId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ vendorId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const vendor = await vendors.findOne(vendorCond, { session });
          if (vendor) {
            // Reverse: debit => vendor ke taka deya (due kombe) -> deletion: due barbe
            // Reverse: credit => vendor theke taka neya (due barbe) -> deletion: due kombe
            const vendorDueDelta = transactionType === 'debit' ? numericAmount : (transactionType === 'credit' ? -numericAmount : 0);
            const vendorUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: vendorDueDelta } };
            if (isHajjCategory) {
              vendorUpdate.$inc.hajDue = (vendorUpdate.$inc.hajDue || 0) + vendorDueDelta;
            }
            if (isUmrahCategory) {
              vendorUpdate.$inc.umrahDue = (vendorUpdate.$inc.umrahDue || 0) + vendorDueDelta;
            }
            if (transactionType === 'debit') {
              vendorUpdate.$inc.totalPaid = (vendorUpdate.$inc.totalPaid || 0) - numericAmount;
            }
            await vendors.updateOne({ _id: vendor._id }, vendorUpdate, { session });
            
            // Clamp negatives
            const after = await vendors.findOne({ _id: vendor._id }, { session });
            const setClamp = {};
            if ((after.totalDue || 0) < 0) setClamp.totalDue = 0;
            if ((after.hajDue !== undefined) && after.hajDue < 0) setClamp.hajDue = 0;
            if ((after.umrahDue !== undefined) && after.umrahDue < 0) setClamp.umrahDue = 0;
            if (Object.keys(setClamp).length) {
              setClamp.updatedAt = new Date();
              await vendors.updateOne({ _id: vendor._id }, { $set: setClamp }, { session });
            }
          }
        }

        // 2.3 Customer (check if airCustomer, otherCustomer, or regular customer)
        if (partyType === 'customer') {
          const customerCond = isValidObjectId
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          
          // Try airCustomers first
          let customer = await airCustomers.findOne(customerCond, { session });
          let isAirCustomer = !!customer;
          let customerCollection = airCustomers;
          
          // If not found in airCustomers, try otherCustomers (Additional Services customers)
          if (!customer) {
            try {
              const otherCustomerCond = isValidObjectId
                ? { $or: [{ customerId: partyId }, { id: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
                : { $or: [{ customerId: partyId }, { id: partyId }, { _id: partyId }], isActive: { $ne: false } };
              customer = await otherCustomers.findOne(otherCustomerCond, { session });
              if (customer) {
                customerCollection = otherCustomers;
              }
            } catch (e) {
              // otherCustomers collection error, continue
              console.warn('Error searching otherCustomers in DELETE:', e.message);
            }
          }
          
          if (customer) {
            const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: dueDelta } };
            if (isHajjCategory) {
              customerUpdate.$inc.hajjDue = (customerUpdate.$inc.hajjDue || 0) + dueDelta;
            }
            if (isUmrahCategory) {
              customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) + dueDelta;
            }
            if (transactionType === 'credit') {
              customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) - numericAmount;
            }
            if (isAirCustomer && transactionType === 'debit') {
              customerUpdate.$inc.totalAmount = (customerUpdate.$inc.totalAmount || 0) - numericAmount;
            }
            await customerCollection.updateOne({ _id: customer._id }, customerUpdate, { session });
            
            // Clamp negatives
            const after = await customerCollection.findOne({ _id: customer._id }, { session });
            const setClamp = {};
            if ((after.totalDue || 0) < 0) setClamp.totalDue = 0;
            if ((after.paidAmount || 0) < 0) setClamp.paidAmount = 0;
            if ((after.hajjDue !== undefined) && after.hajjDue < 0) setClamp.hajjDue = 0;
            if ((after.umrahDue !== undefined) && after.umrahDue < 0) setClamp.umrahDue = 0;
            if (typeof after.totalAmount === 'number' && typeof after.paidAmount === 'number' && after.paidAmount > after.totalAmount) {
              setClamp.paidAmount = after.totalAmount;
            }
            if (Object.keys(setClamp).length) {
              setClamp.updatedAt = new Date();
              await customerCollection.updateOne({ _id: customer._id }, { $set: setClamp }, { session });
            }

            // Reverse haji paidAmount if credit transaction
            if (transactionType === 'credit') {
              const hajiCond = isValidObjectId
                ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
                : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
              const hajiDoc = await haji.findOne(hajiCond, { session });
              if (hajiDoc && hajiDoc._id) {
                await haji.updateOne(
                  { _id: hajiDoc._id },
                  { $inc: { paidAmount: -numericAmount }, $set: { updatedAt: new Date() } },
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
                await triggerFamilyRecomputeForHaji(afterH, { session });
              }
              
              // Reverse umrah paidAmount if credit transaction
              const umrahCond = isValidObjectId
                ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }] }
                : { $or: [{ customerId: partyId }, { _id: partyId }] };
              const umrahDoc = await umrah.findOne(umrahCond, { session });
              if (umrahDoc && umrahDoc._id) {
                await umrah.updateOne(
                  { _id: umrahDoc._id },
                  { $inc: { paidAmount: -numericAmount }, $set: { updatedAt: new Date() } },
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
        }

        // 2.4 Haji
        if (partyType === 'haji') {
          const hajiCond = isValidObjectId
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const hajiDoc = await haji.findOne(hajiCond, { session });
          if (hajiDoc && transactionType === 'credit') {
            await haji.updateOne(
              { _id: hajiDoc._id },
              { $inc: { paidAmount: -numericAmount }, $set: { updatedAt: new Date() } },
              { session }
            );
            const afterHaji = await haji.findOne({ _id: hajiDoc._id }, { session });
            const setClampHaji = {};
            if ((afterHaji.paidAmount || 0) < 0) setClampHaji.paidAmount = 0;
            if (typeof afterHaji.totalAmount === 'number' && typeof afterHaji.paidAmount === 'number' && afterHaji.paidAmount > afterHaji.totalAmount) {
              setClampHaji.paidAmount = afterHaji.totalAmount;
            }
            if (Object.keys(setClampHaji).length) {
              setClampHaji.updatedAt = new Date();
              await haji.updateOne({ _id: hajiDoc._id }, { $set: setClampHaji }, { session });
            }
            await triggerFamilyRecomputeForHaji(afterHaji, { session });
          }

          // Reverse sync to linked customer
          try {
            if (hajiDoc && hajiDoc._id) {
              const linkedCustomerId = hajiDoc.customerId || hajiDoc.customer_id;
              if (linkedCustomerId) {
              const customerCond = ObjectId.isValid(linkedCustomerId)
                ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: { $ne: false } }
                : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: { $ne: false } };
              const custDoc = await airCustomers.findOne(customerCond, { session });
              if (custDoc && custDoc._id) {
                const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: -dueDelta } };
                if (isHajjCategory) customerUpdate.$inc.hajjDue = (customerUpdate.$inc.hajjDue || 0) - dueDelta;
                if (isUmrahCategory) customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) - dueDelta;
                if (transactionType === 'credit') customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) - numericAmount;

                await airCustomers.updateOne({ _id: custDoc._id }, customerUpdate, { session });

                // Clamp negatives
                const afterCust = await airCustomers.findOne({ _id: custDoc._id }, { session });
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
                  await airCustomers.updateOne({ _id: custDoc._id }, { $set: clampCust }, { session });
                }
              }
            }
            }
          } catch (syncErr) {
            console.warn('Customer sync reversal from haji transaction failed:', syncErr?.message);
          }
        }

        // 2.5 Umrah
        if (partyType === 'umrah') {
          const umrahCond = isValidObjectId
            ? { $or: [{ customerId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ customerId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const umrahDoc = await umrah.findOne(umrahCond, { session });
          if (umrahDoc && transactionType === 'credit') {
            await umrah.updateOne(
              { _id: umrahDoc._id },
              { $inc: { paidAmount: -numericAmount }, $set: { updatedAt: new Date() } },
              { session }
            );
            const afterUmrah = await umrah.findOne({ _id: umrahDoc._id }, { session });
            const setClampUmrah = {};
            if ((afterUmrah.paidAmount || 0) < 0) setClampUmrah.paidAmount = 0;
            if (typeof afterUmrah.totalAmount === 'number' && typeof afterUmrah.paidAmount === 'number' && afterUmrah.paidAmount > afterUmrah.totalAmount) {
              setClampUmrah.paidAmount = afterUmrah.totalAmount;
            }
            if (Object.keys(setClampUmrah).length) {
              setClampUmrah.updatedAt = new Date();
              await umrah.updateOne({ _id: umrahDoc._id }, { $set: setClampUmrah }, { session });
            }
            await triggerFamilyRecomputeForUmrah(afterUmrah, { session });
          }

          // Reverse sync to linked customer
          try {
            if (umrahDoc && umrahDoc._id) {
              const linkedCustomerId = umrahDoc.customerId || umrahDoc.customer_id;
              if (linkedCustomerId) {
              const customerCond = ObjectId.isValid(linkedCustomerId)
                ? { $or: [{ _id: new ObjectId(linkedCustomerId) }, { customerId: linkedCustomerId }], isActive: { $ne: false } }
                : { $or: [{ _id: linkedCustomerId }, { customerId: linkedCustomerId }], isActive: { $ne: false } };
              const custDoc = await airCustomers.findOne(customerCond, { session });
              if (custDoc && custDoc._id) {
                const customerUpdate = { $set: { updatedAt: new Date() }, $inc: { totalDue: -dueDelta } };
                if (isUmrahCategory) customerUpdate.$inc.umrahDue = (customerUpdate.$inc.umrahDue || 0) - dueDelta;
                if (transactionType === 'credit') customerUpdate.$inc.paidAmount = (customerUpdate.$inc.paidAmount || 0) - numericAmount;

                await airCustomers.updateOne({ _id: custDoc._id }, customerUpdate, { session });

                // Clamp negatives
                const afterCust = await airCustomers.findOne({ _id: custDoc._id }, { session });
                const clampCust = {};
                if ((afterCust.totalDue || 0) < 0) clampCust.totalDue = 0;
                if ((afterCust.paidAmount || 0) < 0) clampCust.paidAmount = 0;
                if ((afterCust.umrahDue !== undefined) && afterCust.umrahDue < 0) clampCust.umrahDue = 0;
                if (typeof afterCust.totalAmount === 'number' && typeof afterCust.paidAmount === 'number' && afterCust.paidAmount > afterCust.totalAmount) {
                  clampCust.paidAmount = afterCust.totalAmount;
                }
                if (Object.keys(clampCust).length) {
                  clampCust.updatedAt = new Date();
                  await airCustomers.updateOne({ _id: custDoc._id }, { $set: clampCust }, { session });
                }
              }
            }
            }
          } catch (syncErr) {
            console.warn('Customer sync reversal from umrah transaction failed:', syncErr?.message);
          }
        }

        // 2.6 Loan
        if (partyType === 'loan') {
          const loanCond = isValidObjectId
            ? { $or: [{ loanId: partyId }, { _id: new ObjectId(partyId) }], isActive: { $ne: false } }
            : { $or: [{ loanId: partyId }, { _id: partyId }], isActive: { $ne: false } };
          const loanDoc = await loans.findOne(loanCond, { session });
          if (loanDoc) {
            const isReceivingLoan = String(loanDoc.loanDirection || '').toLowerCase() === 'receiving';
            let dueDelta = 0;
            const loanUpdate = { $set: { updatedAt: new Date() }, $inc: {} };

            if (isReceivingLoan) {
              // Reverse: credit = principal in -> deletion: principal out
              if (transactionType === 'credit') {
                dueDelta = -numericAmount;
                loanUpdate.$inc.totalAmount = (loanUpdate.$inc.totalAmount || 0) - numericAmount;
              } else if (transactionType === 'debit') {
                dueDelta = numericAmount;
                loanUpdate.$inc.paidAmount = (loanUpdate.$inc.paidAmount || 0) - numericAmount;
              }
            } else {
              // Reverse: debit = principal out -> deletion: principal in
              if (transactionType === 'debit') {
                dueDelta = -numericAmount;
                loanUpdate.$inc.totalAmount = (loanUpdate.$inc.totalAmount || 0) - numericAmount;
              } else if (transactionType === 'credit') {
                dueDelta = numericAmount;
                loanUpdate.$inc.paidAmount = (loanUpdate.$inc.paidAmount || 0) - numericAmount;
              }
            }

            loanUpdate.$inc.totalDue = dueDelta;
            await loans.updateOne({ _id: loanDoc._id }, loanUpdate, { session });
            
            // Clamp negatives
            const afterLoan = await loans.findOne({ _id: loanDoc._id }, { session });
            const clampLoan = {};
            if ((afterLoan.totalDue || 0) < 0) clampLoan.totalDue = 0;
            if ((afterLoan.paidAmount || 0) < 0) clampLoan.paidAmount = 0;
            if ((afterLoan.totalAmount || 0) < 0) clampLoan.totalAmount = 0;
            if (typeof afterLoan.totalAmount === 'number' && typeof afterLoan.paidAmount === 'number' && afterLoan.paidAmount > afterLoan.totalAmount) {
              clampLoan.paidAmount = afterLoan.totalAmount;
            }
            if (Object.keys(clampLoan).length) {
              clampLoan.updatedAt = new Date();
              await loans.updateOne({ _id: loanDoc._id }, { $set: clampLoan }, { session });
            }
          }
        }
      }

      // 3. Reverse operating expense category updates
      if (tx.operatingExpenseCategoryId && ObjectId.isValid(String(tx.operatingExpenseCategoryId)) && transactionType === 'debit') {
        await operatingExpenseCategories.updateOne(
          { _id: new ObjectId(String(tx.operatingExpenseCategoryId)) },
          { 
            $inc: { totalAmount: -numericAmount, itemCount: -1 }, 
            $set: { lastUpdated: new Date().toISOString().slice(0, 10) } 
          },
          { session }
        );
      }

      // 4. Unlink money exchange records
      if ((partyType === 'money-exchange' || partyType === 'money_exchange') && tx.transactionId) {
        try {
          const exchangeId = tx.partyId && ObjectId.isValid(tx.partyId) ? new ObjectId(tx.partyId) : null;
          if (exchangeId) {
            await exchanges.updateOne(
              { _id: exchangeId },
              { 
                $set: { 
                  transactionId: null,
                  transactionLinked: false,
                  updatedAt: new Date() 
                } 
              },
              { session }
            );
          }
        } catch (exchangeErr) {
          console.warn('Failed to unlink exchange from transaction:', exchangeErr?.message);
        }
      }

      // 5. Revert invoice status (optional - you may want to keep it as 'paid' or revert to 'pending')
      // Uncomment if you want to revert invoice status on transaction deletion
      /*
      if (tx.invoiceId) {
        try {
          const invoiceQuery = ObjectId.isValid(tx.invoiceId)
            ? { $or: [{ invoiceId: tx.invoiceId }, { _id: new ObjectId(tx.invoiceId) }], isActive: { $ne: false } }
            : { $or: [{ invoiceId: tx.invoiceId }, { _id: tx.invoiceId }], isActive: { $ne: false } };
          const invoiceDoc = await invoices.findOne(invoiceQuery, { session });
          if (invoiceDoc) {
            await invoices.updateOne(
              { _id: invoiceDoc._id },
              { $set: { status: 'pending', updatedAt: new Date() } },
              { session }
            );
          }
        } catch (invoiceErr) {
          console.warn('Failed to revert invoice status:', invoiceErr?.message);
        }
      }
      */

      // 6. Reverse farm income/expense updates
      if (partyType === 'miraj-income' && tx.partyId) {
        try {
          await farmIncomes.updateOne(
            { id: Number(tx.partyId) },
            { $set: { amount: 0, updatedAt: new Date() } },
            { session }
          );
        } catch (farmErr) {
          console.warn('Failed to reverse farm income:', farmErr?.message);
        }
      } else if (partyType === 'miraj-expense' && tx.partyId) {
        try {
          await farmExpenses.updateOne(
            { id: Number(tx.partyId) },
            { $set: { amount: 0, updatedAt: new Date() } },
            { session }
          );
        } catch (farmErr) {
          console.warn('Failed to reverse farm expense:', farmErr?.message);
        }
      }

      // 7. Delete the transaction
      const deleteResult = await transactions.deleteOne({ _id: new ObjectId(id) }, { session });
      if (deleteResult.deletedCount === 0) {
        throw new Error("Failed to delete transaction");
      }

      // Commit transaction
      await session.commitTransaction();

      res.json({
        success: true,
        message: "Transaction deleted successfully"
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

    console.error('Transaction deletion error:', err);
    res.status(500).json({
      success: false,
      message: err.message || "Failed to delete transaction"
    });
  } finally {
    // End session
    if (session) {
      session.endSession();
    }
  }
});




// ==================== DIRECT TRANSACTIONS API: PERSONAL EXPENSE ====================
// Use main `transactions` collection, but mark as scope: "personal-expense" and type: "expense"

// GET transactions (filters: from, to, categoryId)
app.get("/api/transactions/personal-expense", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { from, to, categoryId } = req.query || {};
    const filter = { scope: "personal-expense", type: "expense" };
    if (from || to) {
      filter.date = {};
      if (from) filter.date.$gte = String(from);
      if (to) filter.date.$lte = String(to);
    }
    if (categoryId && ObjectId.isValid(String(categoryId))) {
      filter.categoryId = String(categoryId);
    }
    const list = await transactions.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map((doc) => ({
      id: String(doc._id || doc.id || ""),
      date: doc.date || new Date().toISOString().slice(0, 10),
      amount: Number(doc.amount || 0),
      categoryId: String(doc.categoryId || ""),
      categoryName: String(doc.categoryName || ""),
      description: String(doc.description || ""),
      tags: Array.isArray(doc.tags) ? doc.tags : [],
      createdAt: doc.createdAt || null
    })));
  } catch (err) {
    console.error("GET /api/transactions/personal-expense error:", err);
    return res.status(500).json({ error: true, message: "Failed to load transactions" });
  }
});

// CREATE transaction (debit-only: increase category totals, no account balance effect)
app.post("/api/transactions/personal-expense", async (req, res) => {
  try {
    const { date, amount, categoryId, description = "", tags = [] } = req.body || {};

    const todayStr = new Date().toISOString().slice(0, 10);
    const txDate = String(date || todayStr).slice(0, 10);
    const numericAmount = Number(amount || 0);
    if (!(numericAmount > 0)) {
      return res.status(400).json({ error: true, message: "Amount must be greater than 0" });
    }
    if (!categoryId || !ObjectId.isValid(String(categoryId))) {
      return res.status(400).json({ error: true, message: "Valid categoryId is required" });
    }

    const cat = await personalExpenseCategories.findOne({ _id: new ObjectId(String(categoryId)) });
    if (!cat) {
      return res.status(404).json({ error: true, message: "Category not found" });
    }

    const doc = {
      scope: "personal-expense",
      type: "expense",
      date: txDate,
      amount: numericAmount,
      categoryId: String(categoryId),
      categoryName: String(cat.name || ""),
      description: String(description || ""),
      tags: Array.isArray(tags) ? tags.map((t) => String(t)) : [],
      createdAt: new Date().toISOString()
    };

    // Effect: increase category totals
    await personalExpenseCategories.updateOne(
      { _id: new ObjectId(String(categoryId)) },
      { $inc: { totalAmount: numericAmount }, $set: { lastUpdated: txDate } }
    );

    const result = await transactions.insertOne(doc);
    const created = await transactions.findOne({ _id: result.insertedId });
    return res.status(201).json({
      id: String(created._id),
      date: created.date,
      amount: created.amount,
      categoryId: created.categoryId,
      categoryName: created.categoryName,
      description: created.description,
      tags: created.tags,
      createdAt: created.createdAt
    });
  } catch (err) {
    console.error("POST /api/transactions/personal-expense error:", err);
    return res.status(500).json({ error: true, message: "Failed to create transaction" });
  }
});

// DELETE transaction (reverse category totals)
app.delete("/api/transactions/personal-expense/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid transaction id" });
    }
    const tx = await transactions.findOne({ _id: new ObjectId(id), scope: "personal-expense", type: "expense" });
    if (!tx) {
      return res.status(404).json({ error: true, message: "Transaction not found" });
    }

    const amount = Number(tx.amount || 0);
    const categoryId = tx.categoryId ? String(tx.categoryId) : null;
    const txDate = tx.date || new Date().toISOString().slice(0, 10);

    if (categoryId && ObjectId.isValid(categoryId) && amount > 0) {
      await personalExpenseCategories.updateOne(
        { _id: new ObjectId(categoryId) },
        { $inc: { totalAmount: -amount }, $set: { lastUpdated: txDate } }
      );
    }

    const result = await transactions.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Transaction not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/transactions/personal-expense/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete transaction" });
  }
});

// ==================== CATTLE MANAGEMENT (CRUD) ====================
const normalizeCattle = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cattleId: doc.id || "",
  name: doc.name || "",
  breed: doc.breed || "",
  age: typeof doc.age === 'number' ? doc.age : Number(doc.age || 0),
  weight: typeof doc.weight === 'number' ? doc.weight : Number(doc.weight || 0),
  purchaseDate: doc.purchaseDate || "",
  healthStatus: doc.healthStatus || "healthy",
  image: doc.image || null,
  imagePublicId: doc.imagePublicId || "",
  gender: doc.gender || "female",
  color: doc.color || "",
  tagNumber: doc.tagNumber || "",
  purchasePrice: typeof doc.purchasePrice === 'number' ? doc.purchasePrice : Number(doc.purchasePrice || 0),
  vendor: doc.vendor || "",
  notes: doc.notes || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

// CREATE cattle
app.post("/api/cattle", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }

    const {
      id,
      name,
      breed = "",
      age = 0,
      weight = 0,
      purchaseDate = "",
      healthStatus = "healthy",
      image = null,
      imagePublicId = "",
      gender = "female",
      color = "",
      tagNumber = "",
      purchasePrice = 0,
      vendor = "",
      notes = ""
    } = req.body || {};

    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Name is required" });
    }

    const now = new Date().toISOString();
    const doc = {
      id: id ? String(id) : undefined,
      name: String(name).trim(),
      breed: String(breed || ""),
      age: Number(age || 0),
      weight: Number(weight || 0),
      purchaseDate: String(purchaseDate || ""),
      healthStatus: String(healthStatus || "healthy"),
      image: image || null,
      imagePublicId: String(imagePublicId || ""),
      gender: String(gender || "female"),
      color: String(color || ""),
      tagNumber: String(tagNumber || ""),
      purchasePrice: Number(purchasePrice || 0),
      vendor: String(vendor || ""),
      notes: String(notes || ""),
      createdAt: now,
      updatedAt: now
    };

    const result = await cattle.insertOne(doc);
    const created = await cattle.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeCattle(created));
  } catch (err) {
    console.error("POST /api/cattle error:", err);
    return res.status(500).json({ error: true, message: "Failed to create cattle" });
  }
});

// GET all cattle (simple, can be extended with filters later)
app.get("/api/cattle", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await cattle.find({}).sort({ createdAt: -1 }).toArray();
    return res.json(list.map(normalizeCattle));
  } catch (err) {
    console.error("GET /api/cattle error:", err);
    return res.status(500).json({ error: true, message: "Failed to load cattle" });
  }
});

// GET one cattle by id
app.get("/api/cattle/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid cattle id" });
    }
    const doc = await cattle.findOne({ _id: new ObjectId(id) });
    if (!doc) return res.status(404).json({ error: true, message: "Cattle not found" });
    // Attach milk data for this cattle (recent first)
    const records = await milkProductions.find({ cattleId: String(doc._id) }).sort({ date: -1 }).limit(200).toArray();
    const milk = records.map((r) => ({
      id: String(r._id || r.id || ""),
      cattleId: String(r.cattleId || ""),
      cattleName: r.cattleName || doc.name || "",
      date: r.date || new Date().toISOString().slice(0, 10),
      morningQuantity: Number(r.morningQuantity || 0),
      afternoonQuantity: Number(r.afternoonQuantity || 0),
      eveningQuantity: Number(r.eveningQuantity || 0),
      totalQuantity: Number(r.totalQuantity || 0),
      quality: r.quality || "good",
      notes: r.notes || ""
    }));
    // Attach health, vaccinations, vet visits (recent first)
    const [healthDocs, vaccDocs, visitDocs] = await Promise.all([
      healthRecords.find({ cattleId: String(doc._id) }).sort({ date: -1 }).limit(200).toArray(),
      vaccinations.find({ cattleId: String(doc._id) }).sort({ date: -1 }).limit(200).toArray(),
      vetVisits.find({ cattleId: String(doc._id) }).sort({ date: -1 }).limit(200).toArray()
    ]);
    const health = healthDocs.map((h) => ({
      id: String(h._id || h.id || ""),
      cattleId: String(h.cattleId || ""),
      cattleName: doc.name || "",
      date: h.date || new Date().toISOString().slice(0, 10),
      condition: h.condition || "",
      symptoms: h.symptoms || "",
      treatment: h.treatment || "",
      medication: h.medication || "",
      dosage: h.dosage || "",
      duration: h.duration || "",
      vetName: h.vetName || "",
      notes: h.notes || "",
      status: h.status || "under_treatment"
    }));
    const vaccinationRecords = vaccDocs.map((v) => ({
      id: String(v._id || v.id || ""),
      cattleId: String(v.cattleId || ""),
      cattleName: doc.name || "",
      vaccineName: v.vaccineName || "",
      date: v.date || new Date().toISOString().slice(0, 10),
      nextDueDate: v.nextDueDate || "",
      batchNumber: v.batchNumber || "",
      vetName: v.vetName || "",
      notes: v.notes || "",
      status: v.status || "completed"
    }));
    const vetVisitRecords = visitDocs.map((vv) => ({
      id: String(vv._id || vv.id || ""),
      cattleId: String(vv.cattleId || ""),
      cattleName: doc.name || "",
      date: vv.date || new Date().toISOString().slice(0, 10),
      visitType: vv.visitType || "",
      vetName: vv.vetName || "",
      clinic: vv.clinic || "",
      purpose: vv.purpose || "",
      diagnosis: vv.diagnosis || "",
      treatment: vv.treatment || "",
      followUpDate: vv.followUpDate || "",
      cost: Number(vv.cost || 0),
      notes: vv.notes || ""
    }));
    // Attach breeding and calving
    const [breedingDocs, calvingDocs] = await Promise.all([
      breedings.find({ cowId: String(doc._id) }).sort({ breedingDate: -1 }).limit(200).toArray(),
      calvings.find({ cowId: String(doc._id) }).sort({ calvingDate: -1 }).limit(200).toArray()
    ]);
    const breedingRecords = breedingDocs.map((b) => ({
      id: String(b._id || b.id || ""),
      cowId: String(b.cowId || ""),
      cowName: doc.name || "",
      bullId: b.bullId || "",
      bullName: b.bullName || "",
      breedingDate: b.breedingDate || "",
      method: b.method || "natural",
      success: b.success || "pending",
      notes: b.notes || "",
      expectedCalvingDate: b.expectedCalvingDate || ""
    }));
    const calvingRecords = calvingDocs.map((c) => ({
      id: String(c._id || c.id || ""),
      cowId: String(c.cowId || ""),
      cowName: doc.name || "",
      calvingDate: c.calvingDate || "",
      calfGender: c.calfGender || "",
      calfWeight: Number(c.calfWeight || 0),
      calfHealth: c.calfHealth || "healthy",
      calvingType: c.calvingType || "normal",
      complications: c.complications || "",
      notes: c.notes || "",
      calfId: c.calfId || ""
    }));
    return res.json({ ...normalizeCattle(doc), milkRecords: milk, healthRecords: health, vaccinationRecords, vetVisitRecords, breedingRecords, calvingRecords });
  } catch (err) {
    console.error("GET /api/cattle/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to load cattle" });
  }
});

// UPDATE cattle
app.put("/api/cattle/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid cattle id" });
    }

    const allowed = [
      "id","name","breed","age","weight","purchaseDate","healthStatus","image","imagePublicId","gender","color","tagNumber","purchasePrice","vendor","notes"
    ];
    const updates = Object.fromEntries(
      Object.entries(req.body || {}).filter(([k]) => allowed.includes(k))
    );

    if (Object.prototype.hasOwnProperty.call(updates, 'name') && !String(updates.name).trim()) {
      return res.status(400).json({ error: true, message: "Name cannot be empty" });
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: true, message: "No valid fields to update" });
    }

    if (typeof updates.age !== 'undefined') updates.age = Number(updates.age || 0);
    if (typeof updates.weight !== 'undefined') updates.weight = Number(updates.weight || 0);
    if (typeof updates.purchasePrice !== 'undefined') updates.purchasePrice = Number(updates.purchasePrice || 0);

    updates.updatedAt = new Date().toISOString();

    const result = await cattle.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnDocument: 'after' }
    );

    if (!result || !result.value) {
      return res.status(404).json({ error: true, message: "Cattle not found" });
    }

    return res.json(normalizeCattle(result.value));
  } catch (err) {
    console.error("PUT /api/cattle/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update cattle" });
  }
});

// DELETE cattle
app.delete("/api/cattle/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid cattle id" });
    }
    const result = await cattle.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Cattle not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/cattle/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete cattle" });
  }
});

// ==================== MILK PRODUCTION (CRUD) ====================
const normalizeMilkRecord = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cattleId: String(doc.cattleId || ""),
  cattleName: doc.cattleName || "",
  date: doc.date || new Date().toISOString().slice(0, 10),
  morningQuantity: Number(doc.morningQuantity || 0),
  afternoonQuantity: Number(doc.afternoonQuantity || 0),
  eveningQuantity: Number(doc.eveningQuantity || 0),
  totalQuantity: Number(doc.totalQuantity || 0),
  quality: doc.quality || "good",
  notes: doc.notes || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

// CREATE milk record
app.post("/api/milk", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }

    const { cattleId, date, morningQuantity = 0, afternoonQuantity = 0, eveningQuantity = 0, quality = "good", notes = "" } = req.body || {};

    if (!cattleId || !ObjectId.isValid(String(cattleId))) {
      return res.status(400).json({ error: true, message: "Valid cattleId is required" });
    }

    const cow = await cattle.findOne({ _id: new ObjectId(String(cattleId)) });
    if (!cow) {
      return res.status(404).json({ error: true, message: "Cattle not found" });
    }

    const d = String(date || new Date().toISOString().slice(0, 10)).slice(0, 10);
    const m = Number(morningQuantity || 0);
    const a = Number(afternoonQuantity || 0);
    const e = Number(eveningQuantity || 0);
    const total = Number((m + a + e).toFixed(1));

    const now = new Date().toISOString();
    const doc = {
      cattleId: String(cow._id),
      cattleName: String(cow.name || ""),
      date: d,
      morningQuantity: m,
      afternoonQuantity: a,
      eveningQuantity: e,
      totalQuantity: total,
      quality: String(quality || "good"),
      notes: String(notes || ""),
      createdAt: now,
      updatedAt: now
    };

    const result = await milkProductions.insertOne(doc);
    const created = await milkProductions.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeMilkRecord(created));
  } catch (err) {
    console.error("POST /api/milk error:", err);
    return res.status(500).json({ error: true, message: "Failed to create milk record" });
  }
});

// GET milk records (filters: cattleId, from, to)
app.get("/api/milk", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { cattleId, from, to } = req.query || {};
    const filter = {};
    if (cattleId && ObjectId.isValid(String(cattleId))) filter.cattleId = String(cattleId);
    if (from || to) {
      filter.date = {};
      if (from) filter.date.$gte = String(from).slice(0, 10);
      if (to) filter.date.$lte = String(to).slice(0, 10);
    }
    const list = await milkProductions.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeMilkRecord));
  } catch (err) {
    console.error("GET /api/milk error:", err);
    return res.status(500).json({ error: true, message: "Failed to load milk records" });
  }
});

// GET one milk record by id
app.get("/api/milk/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid record id" });
    }
    const doc = await milkProductions.findOne({ _id: new ObjectId(id) });
    if (!doc) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json(normalizeMilkRecord(doc));
  } catch (err) {
    console.error("GET /api/milk/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to load record" });
  }
});

// UPDATE milk record
app.put("/api/milk/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid record id" });
    }

    const allowed = ["date","morningQuantity","afternoonQuantity","eveningQuantity","quality","notes"];
    const updates = Object.fromEntries(
      Object.entries(req.body || {}).filter(([k]) => allowed.includes(k))
    );
    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: true, message: "No valid fields to update" });
    }

    if (typeof updates.morningQuantity !== 'undefined') updates.morningQuantity = Number(updates.morningQuantity || 0);
    if (typeof updates.afternoonQuantity !== 'undefined') updates.afternoonQuantity = Number(updates.afternoonQuantity || 0);
    if (typeof updates.eveningQuantity !== 'undefined') updates.eveningQuantity = Number(updates.eveningQuantity || 0);
    if (typeof updates.date !== 'undefined') updates.date = String(updates.date).slice(0, 10);

    // recompute total
    const existing = await milkProductions.findOne({ _id: new ObjectId(id) });
    if (!existing) return res.status(404).json({ error: true, message: "Record not found" });
    const m = typeof updates.morningQuantity !== 'undefined' ? updates.morningQuantity : Number(existing.morningQuantity || 0);
    const a = typeof updates.afternoonQuantity !== 'undefined' ? updates.afternoonQuantity : Number(existing.afternoonQuantity || 0);
    const e = typeof updates.eveningQuantity !== 'undefined' ? updates.eveningQuantity : Number(existing.eveningQuantity || 0);
    updates.totalQuantity = Number((m + a + e).toFixed(1));
    updates.updatedAt = new Date().toISOString();

    const result = await milkProductions.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: updates },
      { returnDocument: 'after' }
    );
    if (!result || !result.value) {
      return res.status(404).json({ error: true, message: "Record not found" });
    }
    return res.json(normalizeMilkRecord(result.value));
  } catch (err) {
    console.error("PUT /api/milk/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update record" });
  }
});

// DELETE milk record
app.delete("/api/milk/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid record id" });
    }
    const result = await milkProductions.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Record not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/milk/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete record" });
  }
});

// ==================== HEALTH RECORDS / VACCINATIONS / VET VISITS (CRUD) ====================
const normalizeHealth = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cattleId: String(doc.cattleId || ""),
  cattleName: doc.cattleName || "",
  date: doc.date || new Date().toISOString().slice(0, 10),
  condition: doc.condition || "",
  symptoms: doc.symptoms || "",
  treatment: doc.treatment || "",
  medication: doc.medication || "",
  dosage: doc.dosage || "",
  duration: doc.duration || "",
  vetName: doc.vetName || "",
  notes: doc.notes || "",
  status: doc.status || "under_treatment",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

const normalizeVaccination = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cattleId: String(doc.cattleId || ""),
  cattleName: doc.cattleName || "",
  vaccineName: doc.vaccineName || "",
  date: doc.date || new Date().toISOString().slice(0, 10),
  nextDueDate: doc.nextDueDate || "",
  batchNumber: doc.batchNumber || "",
  vetName: doc.vetName || "",
  notes: doc.notes || "",
  status: doc.status || "completed",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

const normalizeVetVisit = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cattleId: String(doc.cattleId || ""),
  cattleName: doc.cattleName || "",
  date: doc.date || new Date().toISOString().slice(0, 10),
  visitType: doc.visitType || "",
  vetName: doc.vetName || "",
  clinic: doc.clinic || "",
  purpose: doc.purpose || "",
  diagnosis: doc.diagnosis || "",
  treatment: doc.treatment || "",
  followUpDate: doc.followUpDate || "",
  cost: Number(doc.cost || 0),
  notes: doc.notes || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

// CREATE Health record
app.post("/api/health", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { cattleId, date, condition = "", symptoms = "", treatment = "", medication = "", dosage = "", duration = "", vetName = "", notes = "", status = "under_treatment" } = req.body || {};
    if (!cattleId || !ObjectId.isValid(String(cattleId))) {
      return res.status(400).json({ error: true, message: "Valid cattleId is required" });
    }
    if (!date) {
      return res.status(400).json({ error: true, message: "date is required" });
    }
    const cow = await cattle.findOne({ _id: new ObjectId(String(cattleId)) });
    if (!cow) return res.status(404).json({ error: true, message: "Cattle not found" });
    const now = new Date().toISOString();
    const doc = {
      cattleId: String(cow._id),
      cattleName: String(cow.name || ""),
      date: String(date).slice(0, 10),
      condition: String(condition || ""),
      symptoms: String(symptoms || ""),
      treatment: String(treatment || ""),
      medication: String(medication || ""),
      dosage: String(dosage || ""),
      duration: String(duration || ""),
      vetName: String(vetName || ""),
      notes: String(notes || ""),
      status: String(status || "under_treatment"),
      createdAt: now,
      updatedAt: now
    };
    const result = await healthRecords.insertOne(doc);
    const created = await healthRecords.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeHealth(created));
  } catch (err) {
    console.error("POST /api/health error:", err);
    return res.status(500).json({ error: true, message: "Failed to create health record" });
  }
});

app.get("/api/health", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { cattleId, from, to, status, q } = req.query || {};
    const filter = {};
    if (cattleId && ObjectId.isValid(String(cattleId))) filter.cattleId = String(cattleId);
    if (from || to) {
      filter.date = {};
      if (from) filter.date.$gte = String(from).slice(0, 10);
      if (to) filter.date.$lte = String(to).slice(0, 10);
    }
    if (status) filter.status = String(status);
    if (q) {
      const text = String(q).trim();
      filter.$or = [
        { condition: { $regex: text, $options: 'i' } },
        { symptoms: { $regex: text, $options: 'i' } },
        { vetName: { $regex: text, $options: 'i' } }
      ];
    }
    const list = await healthRecords.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeHealth));
  } catch (err) {
    console.error("GET /api/health error:", err);
    return res.status(500).json({ error: true, message: "Failed to load health records" });
  }
});

app.put("/api/health/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const allowed = ["date","condition","symptoms","treatment","medication","dosage","duration","vetName","notes","status"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: true, message: "No valid fields to update" });
    if (typeof updates.date !== 'undefined') updates.date = String(updates.date).slice(0, 10);
    updates.updatedAt = new Date().toISOString();
    const result = await healthRecords.findOneAndUpdate({ _id: new ObjectId(id) }, { $set: updates }, { returnDocument: 'after' });
    if (!result || !result.value) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json(normalizeHealth(result.value));
  } catch (err) {
    console.error("PUT /api/health/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update health record" });
  }
});

app.delete("/api/health/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const result = await healthRecords.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/health/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete health record" });
  }
});

// VACCINATIONS
app.post("/api/vaccinations", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cattleId, vaccineName, date, nextDueDate = "", batchNumber = "", vetName = "", notes = "", status = "completed" } = req.body || {};
    if (!cattleId || !ObjectId.isValid(String(cattleId))) return res.status(400).json({ error: true, message: "Valid cattleId is required" });
    if (!vaccineName || !String(vaccineName).trim()) return res.status(400).json({ error: true, message: "vaccineName is required" });
    if (!date) return res.status(400).json({ error: true, message: "date is required" });
    const cow = await cattle.findOne({ _id: new ObjectId(String(cattleId)) });
    if (!cow) return res.status(404).json({ error: true, message: "Cattle not found" });
    const now = new Date().toISOString();
    const doc = {
      cattleId: String(cow._id),
      cattleName: String(cow.name || ""),
      vaccineName: String(vaccineName).trim(),
      date: String(date).slice(0, 10),
      nextDueDate: String(nextDueDate || ""),
      batchNumber: String(batchNumber || ""),
      vetName: String(vetName || ""),
      notes: String(notes || ""),
      status: String(status || "completed"),
      createdAt: now,
      updatedAt: now
    };
    const result = await vaccinations.insertOne(doc);
    const created = await vaccinations.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeVaccination(created));
  } catch (err) {
    console.error("POST /api/vaccinations error:", err);
    return res.status(500).json({ error: true, message: "Failed to create vaccination" });
  }
});

app.get("/api/vaccinations", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cattleId, from, to, dueBefore } = req.query || {};
    const filter = {};
    if (cattleId && ObjectId.isValid(String(cattleId))) filter.cattleId = String(cattleId);
    if (from || to) {
      filter.date = {};
      if (from) filter.date.$gte = String(from).slice(0, 10);
      if (to) filter.date.$lte = String(to).slice(0, 10);
    }
    if (dueBefore) {
      filter.nextDueDate = { $ne: "" };
      filter.nextDueDate.$lte = String(dueBefore).slice(0, 10);
    }
    const list = await vaccinations.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeVaccination));
  } catch (err) {
    console.error("GET /api/vaccinations error:", err);
    return res.status(500).json({ error: true, message: "Failed to load vaccinations" });
  }
});

app.put("/api/vaccinations/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const allowed = ["vaccineName","date","nextDueDate","batchNumber","vetName","notes","status"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: true, message: "No valid fields to update" });
    if (typeof updates.date !== 'undefined') updates.date = String(updates.date).slice(0, 10);
    if (typeof updates.nextDueDate !== 'undefined') updates.nextDueDate = String(updates.nextDueDate || "");
    updates.updatedAt = new Date().toISOString();
    const result = await vaccinations.findOneAndUpdate({ _id: new ObjectId(id) }, { $set: updates }, { returnDocument: 'after' });
    if (!result || !result.value) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json(normalizeVaccination(result.value));
  } catch (err) {
    console.error("PUT /api/vaccinations/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update vaccination" });
  }
});

app.delete("/api/vaccinations/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const result = await vaccinations.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/vaccinations/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete vaccination" });
  }
});

// VET VISITS
app.post("/api/vet-visits", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cattleId, date, visitType, vetName, clinic = "", purpose = "", diagnosis = "", treatment = "", followUpDate = "", cost = 0, notes = "" } = req.body || {};
    if (!cattleId || !ObjectId.isValid(String(cattleId))) return res.status(400).json({ error: true, message: "Valid cattleId is required" });
    if (!date) return res.status(400).json({ error: true, message: "date is required" });
    if (!visitType || !String(visitType).trim()) return res.status(400).json({ error: true, message: "visitType is required" });
    if (!vetName || !String(vetName).trim()) return res.status(400).json({ error: true, message: "vetName is required" });
    const cow = await cattle.findOne({ _id: new ObjectId(String(cattleId)) });
    if (!cow) return res.status(404).json({ error: true, message: "Cattle not found" });
    const now = new Date().toISOString();
    const doc = {
      cattleId: String(cow._id),
      cattleName: String(cow.name || ""),
      date: String(date).slice(0, 10),
      visitType: String(visitType).trim(),
      vetName: String(vetName).trim(),
      clinic: String(clinic || ""),
      purpose: String(purpose || ""),
      diagnosis: String(diagnosis || ""),
      treatment: String(treatment || ""),
      followUpDate: String(followUpDate || ""),
      cost: Number(cost || 0),
      notes: String(notes || ""),
      createdAt: now,
      updatedAt: now
    };
    const result = await vetVisits.insertOne(doc);
    const created = await vetVisits.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeVetVisit(created));
  } catch (err) {
    console.error("POST /api/vet-visits error:", err);
    return res.status(500).json({ error: true, message: "Failed to create vet visit" });
  }
});

app.get("/api/vet-visits", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cattleId, from, to, q } = req.query || {};
    const filter = {};
    if (cattleId && ObjectId.isValid(String(cattleId))) filter.cattleId = String(cattleId);
    if (from || to) {
      filter.date = {};
      if (from) filter.date.$gte = String(from).slice(0, 10);
      if (to) filter.date.$lte = String(to).slice(0, 10);
    }
    if (q) {
      const text = String(q).trim();
      filter.$or = [
        { visitType: { $regex: text, $options: 'i' } },
        { vetName: { $regex: text, $options: 'i' } },
        { clinic: { $regex: text, $options: 'i' } }
      ];
    }
    const list = await vetVisits.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeVetVisit));
  } catch (err) {
    console.error("GET /api/vet-visits error:", err);
    return res.status(500).json({ error: true, message: "Failed to load vet visits" });
  }
});

app.put("/api/vet-visits/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const allowed = ["date","visitType","vetName","clinic","purpose","diagnosis","treatment","followUpDate","cost","notes"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: true, message: "No valid fields to update" });
    if (typeof updates.date !== 'undefined') updates.date = String(updates.date).slice(0, 10);
    if (typeof updates.followUpDate !== 'undefined') updates.followUpDate = String(updates.followUpDate || "");
    if (typeof updates.cost !== 'undefined') updates.cost = Number(updates.cost || 0);
    updates.updatedAt = new Date().toISOString();
    const result = await vetVisits.findOneAndUpdate({ _id: new ObjectId(id) }, { $set: updates }, { returnDocument: 'after' });
    if (!result || !result.value) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json(normalizeVetVisit(result.value));
  } catch (err) {
    console.error("PUT /api/vet-visits/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update vet visit" });
  }
});

app.delete("/api/vet-visits/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid record id" });
    const result = await vetVisits.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) return res.status(404).json({ error: true, message: "Record not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/vet-visits/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete vet visit" });
  }
});


// ==================== FEED MANAGEMENT (TYPES, STOCK, USAGE) ====================
const normalizeFeedType = (doc) => ({
  id: String(doc._id || doc.id || ""),
  name: doc.name || "",
  type: doc.type || "",
  unit: doc.unit || "kg",
  costPerUnit: Number(doc.costPerUnit || 0),
  supplier: doc.supplier || "",
  description: doc.description || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

const normalizeFeedStock = (doc) => ({
  id: String(doc._id || doc.id || ""),
  feedTypeId: String(doc.feedTypeId || ""),
  feedName: doc.feedName || "",
  currentStock: Number(doc.currentStock || 0),
  minStock: Number(doc.minStock || 0),
  purchaseDate: doc.purchaseDate || "",
  expiryDate: doc.expiryDate || "",
  supplier: doc.supplier || "",
  cost: Number(doc.cost || 0),
  notes: doc.notes || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

const normalizeFeedUsage = (doc) => ({
  id: String(doc._id || doc.id || ""),
  feedTypeId: String(doc.feedTypeId || ""),
  feedName: doc.feedName || "",
  date: doc.date || new Date().toISOString().slice(0, 10),
  quantity: Number(doc.quantity || 0),
  cattleId: doc.cattleId || "",
  purpose: doc.purpose || "",
  notes: doc.notes || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

// FEED TYPES
app.post("/api/feeds/types", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { name, type = "", unit = "kg", costPerUnit = 0, supplier = "", description = "" } = req.body || {};
    if (!name || !String(name).trim()) {
      return res.status(400).json({ error: true, message: "Feed name is required" });
    }
    const exists = await feedTypes.findOne({ name: String(name).trim() }, { collation: { locale: "en", strength: 2 } });
    if (exists) {
      return res.status(409).json({ error: true, message: "A feed with this name already exists" });
    }
    const now = new Date().toISOString();
    const doc = {
      name: String(name).trim(),
      type: String(type || ""),
      unit: String(unit || "kg"),
      costPerUnit: Number(costPerUnit || 0),
      supplier: String(supplier || ""),
      description: String(description || ""),
      createdAt: now,
      updatedAt: now
    };
    const result = await feedTypes.insertOne(doc);
    const created = await feedTypes.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeFeedType(created));
  } catch (err) {
    console.error("POST /api/feeds/types error:", err);
    if (err && err.code === 11000) {
      return res.status(409).json({ error: true, message: "A feed with this name already exists" });
    }
    return res.status(500).json({ error: true, message: "Failed to create feed type" });
  }
});

app.get("/api/feeds/types", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await feedTypes.find({}).sort({ name: 1 }).toArray();
    return res.json(list.map(normalizeFeedType));
  } catch (err) {
    console.error("GET /api/feeds/types error:", err);
    return res.status(500).json({ error: true, message: "Failed to load feed types" });
  }
});

// FEED STOCKS
app.post("/api/feeds/stocks", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { feedTypeId, quantity = 0, purchaseDate, expiryDate = "", supplier = "", cost = 0, notes = "" } = req.body || {};
    if (!feedTypeId || !ObjectId.isValid(String(feedTypeId))) {
      return res.status(400).json({ error: true, message: "Valid feedTypeId is required" });
    }
    const feed = await feedTypes.findOne({ _id: new ObjectId(String(feedTypeId)) });
    if (!feed) {
      return res.status(404).json({ error: true, message: "Feed type not found" });
    }
    if (!purchaseDate) {
      return res.status(400).json({ error: true, message: "purchaseDate is required" });
    }
    const qty = Number(quantity || 0);
    const minStock = Number((qty * 0.3).toFixed(2));
    const now = new Date().toISOString();
    const doc = {
      feedTypeId: String(feed._id),
      feedName: String(feed.name || ""),
      currentStock: qty,
      minStock,
      purchaseDate: String(purchaseDate),
      expiryDate: String(expiryDate || ""),
      supplier: String(supplier || ""),
      cost: Number(cost || 0),
      notes: String(notes || ""),
      createdAt: now,
      updatedAt: now
    };
    const result = await feedStocks.insertOne(doc);
    const created = await feedStocks.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeFeedStock(created));
  } catch (err) {
    console.error("POST /api/feeds/stocks error:", err);
    return res.status(500).json({ error: true, message: "Failed to add feed stock" });
  }
});

app.get("/api/feeds/stocks", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const list = await feedStocks.find({}).sort({ purchaseDate: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeFeedStock));
  } catch (err) {
    console.error("GET /api/feeds/stocks error:", err);
    return res.status(500).json({ error: true, message: "Failed to load feed stocks" });
  }
});

// FEED USAGES
app.post("/api/feeds/usages", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { feedTypeId, date, quantity = 0, cattleId = "", purpose = "", notes = "" } = req.body || {};
    if (!feedTypeId || !ObjectId.isValid(String(feedTypeId))) {
      return res.status(400).json({ error: true, message: "Valid feedTypeId is required" });
    }
    if (!date) {
      return res.status(400).json({ error: true, message: "date is required" });
    }
    const feed = await feedTypes.findOne({ _id: new ObjectId(String(feedTypeId)) });
    if (!feed) {
      return res.status(404).json({ error: true, message: "Feed type not found" });
    }
    const now = new Date().toISOString();
    const doc = {
      feedTypeId: String(feed._id),
      feedName: String(feed.name || ""),
      date: String(date).slice(0, 10),
      quantity: Number(quantity || 0),
      cattleId: String(cattleId || ""),
      purpose: String(purpose || ""),
      notes: String(notes || ""),
      createdAt: now,
      updatedAt: now
    };

    // Decrease stock for the latest stock entry for this feed type (simple approach)
    const latestStock = await feedStocks.findOne({ feedTypeId: String(feed._id) }, { sort: { purchaseDate: -1, createdAt: -1 } });
    if (latestStock) {
      const newStock = Math.max(0, Number(latestStock.currentStock || 0) - Number(doc.quantity));
      await feedStocks.updateOne({ _id: latestStock._id }, { $set: { currentStock: newStock, updatedAt: now } });
    }

    const result = await feedUsages.insertOne(doc);
    const created = await feedUsages.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeFeedUsage(created));
  } catch (err) {
    console.error("POST /api/feeds/usages error:", err);
    return res.status(500).json({ error: true, message: "Failed to add feed usage" });
  }
});

app.get("/api/feeds/usages", async (req, res) => {
  try {
    if (dbConnectionError) {
      return res.status(500).json({ error: true, message: "Database not initialized" });
    }
    const { feedTypeId, date, from, to, q } = req.query || {};
    const filter = {};
    if (feedTypeId && ObjectId.isValid(String(feedTypeId))) filter.feedTypeId = String(feedTypeId);
    if (date) filter.date = String(date).slice(0, 10);
    if (from || to) {
      filter.date = filter.date || {};
      if (from) filter.date.$gte = String(from).slice(0, 10);
      if (to) filter.date.$lte = String(to).slice(0, 10);
    }
    if (q) {
      const text = String(q).trim();
      filter.$or = [
        { feedName: { $regex: text, $options: 'i' } },
        { cattleId: { $regex: text, $options: 'i' } },
        { purpose: { $regex: text, $options: 'i' } }
      ];
    }
    const list = await feedUsages.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeFeedUsage));
  } catch (err) {
    console.error("GET /api/feeds/usages error:", err);
    return res.status(500).json({ error: true, message: "Failed to load feed usages" });
  }
});

app.delete("/api/feeds/usages/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ error: true, message: "Invalid usage id" });
    }
    const usage = await feedUsages.findOne({ _id: new ObjectId(id) });
    if (!usage) {
      return res.status(404).json({ error: true, message: "Usage record not found" });
    }
    // Optionally revert stock (best effort on the latest stock batch)
    const latestStock = await feedStocks.findOne({ feedTypeId: String(usage.feedTypeId) }, { sort: { purchaseDate: -1, createdAt: -1 } });
    if (latestStock) {
      const now = new Date().toISOString();
      const newStock = Number(latestStock.currentStock || 0) + Number(usage.quantity || 0);
      await feedStocks.updateOne({ _id: latestStock._id }, { $set: { currentStock: newStock, updatedAt: now } });
    }
    const result = await feedUsages.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).json({ error: true, message: "Usage record not found" });
    }
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/feeds/usages/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete feed usage" });
  }
});

// ==================== BREEDINGS (CRUD) ====================
const normalizeBreeding = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cowId: String(doc.cowId || ""),
  cowName: doc.cowName || "",
  bullId: doc.bullId || "",
  bullName: doc.bullName || "",
  breedingDate: doc.breedingDate || "",
  method: doc.method || "natural",
  success: doc.success || "pending",
  notes: doc.notes || "",
  expectedCalvingDate: doc.expectedCalvingDate || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

app.post("/api/breedings", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cowId, bullId = "", bullName = "", breedingDate, method = "natural", success = "pending", notes = "", expectedCalvingDate = "" } = req.body || {};
    if (!cowId || !ObjectId.isValid(String(cowId))) return res.status(400).json({ error: true, message: "Valid cowId is required" });
    if (!breedingDate) return res.status(400).json({ error: true, message: "breedingDate is required" });
    const cow = await cattle.findOne({ _id: new ObjectId(String(cowId)) });
    if (!cow) return res.status(404).json({ error: true, message: "Cow not found" });
    const d = String(breedingDate).slice(0, 10);
    const now = new Date().toISOString();
    let expected = String(expectedCalvingDate || "");
    if (!expected) {
      const base = new Date(d);
      if (!isNaN(base.getTime())) {
        const exp = new Date(base.getTime() + 280 * 24 * 60 * 60 * 1000);
        expected = exp.toISOString().slice(0, 10);
      }
    }
    const doc = {
      cowId: String(cow._id),
      cowName: String(cow.name || ""),
      bullId: String(bullId || ""),
      bullName: String(bullName || ""),
      breedingDate: d,
      method: String(method || "natural"),
      success: String(success || "pending"),
      notes: String(notes || ""),
      expectedCalvingDate: expected,
      createdAt: now,
      updatedAt: now
    };
    const result = await breedings.insertOne(doc);
    const created = await breedings.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeBreeding(created));
  } catch (err) {
    console.error("POST /api/breedings error:", err);
    return res.status(500).json({ error: true, message: "Failed to create breeding" });
  }
});

app.get("/api/breedings", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cowId, from, to, method, success, q } = req.query || {};
    const filter = {};
    if (cowId && ObjectId.isValid(String(cowId))) filter.cowId = String(cowId);
    if (from || to) {
      filter.breedingDate = {};
      if (from) filter.breedingDate.$gte = String(from).slice(0, 10);
      if (to) filter.breedingDate.$lte = String(to).slice(0, 10);
    }
    if (method) filter.method = String(method);
    if (success) filter.success = String(success);
    if (q) {
      const text = String(q).trim();
      filter.$or = [
        { cowName: { $regex: text, $options: 'i' } },
        { bullName: { $regex: text, $options: 'i' } },
        { notes: { $regex: text, $options: 'i' } }
      ];
    }
    const list = await breedings.find(filter).sort({ breedingDate: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeBreeding));
  } catch (err) {
    console.error("GET /api/breedings error:", err);
    return res.status(500).json({ error: true, message: "Failed to load breedings" });
  }
});

app.put("/api/breedings/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid breeding id" });
    const allowed = ["bullId","bullName","breedingDate","method","success","notes","expectedCalvingDate"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: true, message: "No valid fields to update" });
    if (typeof updates.breedingDate !== 'undefined') updates.breedingDate = String(updates.breedingDate).slice(0, 10);
    if (typeof updates.expectedCalvingDate !== 'undefined') updates.expectedCalvingDate = String(updates.expectedCalvingDate || "");
    updates.updatedAt = new Date().toISOString();
    const result = await breedings.findOneAndUpdate({ _id: new ObjectId(id) }, { $set: updates }, { returnDocument: 'after' });
    if (!result || !result.value) return res.status(404).json({ error: true, message: "Breeding not found" });
    return res.json(normalizeBreeding(result.value));
  } catch (err) {
    console.error("PUT /api/breedings/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update breeding" });
  }
});

app.delete("/api/breedings/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid breeding id" });
    const result = await breedings.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) return res.status(404).json({ error: true, message: "Breeding not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/breedings/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete breeding" });
  }
});

// ==================== CALVINGS (CRUD) ====================
const normalizeCalving = (doc) => ({
  id: String(doc._id || doc.id || ""),
  cowId: String(doc.cowId || ""),
  cowName: doc.cowName || "",
  calvingDate: doc.calvingDate || "",
  calfGender: doc.calfGender || "",
  calfWeight: Number(doc.calfWeight || 0),
  calfHealth: doc.calfHealth || "healthy",
  calvingType: doc.calvingType || "normal",
  complications: doc.complications || "",
  notes: doc.notes || "",
  calfId: doc.calfId || "",
  createdAt: doc.createdAt || null,
  updatedAt: doc.updatedAt || null
});

app.post("/api/calvings", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cowId, calvingDate, calfGender, calfWeight = 0, calfHealth = "healthy", calvingType = "normal", complications = "", notes = "", calfId = "" } = req.body || {};
    if (!cowId || !ObjectId.isValid(String(cowId))) return res.status(400).json({ error: true, message: "Valid cowId is required" });
    if (!calvingDate) return res.status(400).json({ error: true, message: "calvingDate is required" });
    if (!calfGender) return res.status(400).json({ error: true, message: "calfGender is required" });
    const cow = await cattle.findOne({ _id: new ObjectId(String(cowId)) });
    if (!cow) return res.status(404).json({ error: true, message: "Cow not found" });
    const d = String(calvingDate).slice(0, 10);
    const now = new Date().toISOString();
    const doc = {
      cowId: String(cow._id),
      cowName: String(cow.name || ""),
      calvingDate: d,
      calfGender: String(calfGender),
      calfWeight: Number(calfWeight || 0),
      calfHealth: String(calfHealth || "healthy"),
      calvingType: String(calvingType || "normal"),
      complications: String(complications || ""),
      notes: String(notes || ""),
      calfId: String(calfId || ""),
      createdAt: now,
      updatedAt: now
    };
    const result = await calvings.insertOne(doc);
    const created = await calvings.findOne({ _id: result.insertedId });
    return res.status(201).json(normalizeCalving(created));
  } catch (err) {
    console.error("POST /api/calvings error:", err);
    return res.status(500).json({ error: true, message: "Failed to create calving" });
  }
});

app.get("/api/calvings", async (req, res) => {
  try {
    if (dbConnectionError) return res.status(500).json({ error: true, message: "Database not initialized" });
    const { cowId, from, to, calfHealth, calvingType, q } = req.query || {};
    const filter = {};
    if (cowId && ObjectId.isValid(String(cowId))) filter.cowId = String(cowId);
    if (from || to) {
      filter.calvingDate = {};
      if (from) filter.calvingDate.$gte = String(from).slice(0, 10);
      if (to) filter.calvingDate.$lte = String(to).slice(0, 10);
    }
    if (calfHealth) filter.calfHealth = String(calfHealth);
    if (calvingType) filter.calvingType = String(calvingType);
    if (q) {
      const text = String(q).trim();
      filter.$or = [
        { cowName: { $regex: text, $options: 'i' } },
        { calfId: { $regex: text, $options: 'i' } },
        { notes: { $regex: text, $options: 'i' } }
      ];
    }
    const list = await calvings.find(filter).sort({ calvingDate: -1, createdAt: -1 }).toArray();
    return res.json(list.map(normalizeCalving));
  } catch (err) {
    console.error("GET /api/calvings error:", err);
    return res.status(500).json({ error: true, message: "Failed to load calvings" });
  }
});

app.put("/api/calvings/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid calving id" });
    const allowed = ["calvingDate","calfGender","calfWeight","calfHealth","calvingType","complications","notes","calfId"];
    const updates = Object.fromEntries(Object.entries(req.body || {}).filter(([k]) => allowed.includes(k)));
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: true, message: "No valid fields to update" });
    if (typeof updates.calvingDate !== 'undefined') updates.calvingDate = String(updates.calvingDate).slice(0, 10);
    if (typeof updates.calfWeight !== 'undefined') updates.calfWeight = Number(updates.calfWeight || 0);
    updates.updatedAt = new Date().toISOString();
    const result = await calvings.findOneAndUpdate({ _id: new ObjectId(id) }, { $set: updates }, { returnDocument: 'after' });
    if (!result || !result.value) return res.status(404).json({ error: true, message: "Calving not found" });
    return res.json(normalizeCalving(result.value));
  } catch (err) {
    console.error("PUT /api/calvings/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to update calving" });
  }
});

app.delete("/api/calvings/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) return res.status(400).json({ error: true, message: "Invalid calving id" });
    const result = await calvings.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) return res.status(404).json({ error: true, message: "Calving not found" });
    return res.json({ success: true });
  } catch (err) {
    console.error("DELETE /api/calvings/:id error:", err);
    return res.status(500).json({ error: true, message: "Failed to delete calving" });
  }
});




// Loan Routes      ////

// ✅ POST: Create Loan (Giving) – stores in 'loans' collection and returns generated id
app.post("/loans/giving", async (req, res) => {
  try {
    const body = req.body || {};
    const firstName = (body.firstName || '').trim();
    const lastName = (body.lastName || '').trim();
    if (!firstName || !lastName) {
      return res.status(400).json({ error: true, message: "firstName and lastName are required" });
    }

    const loanDirection = 'giving';
    const loanId = await generateLoanId(db, loanDirection);
    const fullName = `${firstName} ${lastName}`.trim();

    const loanDoc = {
      loanId,
      loanDirection, // 'giving'
      status: 'Active',

      // Personal
      firstName,
      lastName,
      fullName,
      fatherName: body.fatherName || '',
      motherName: body.motherName || '',
      dateOfBirth: body.dateOfBirth || '',
      gender: body.gender || '',
      maritalStatus: body.maritalStatus || '',
      nidNumber: body.nidNumber || '',
      nidFrontImage: body.nidFrontImage || '',
      nidBackImage: body.nidBackImage || '',
      profilePhoto: body.profilePhoto || '',

      // Address
      presentAddress: body.presentAddress || '',
      permanentAddress: body.permanentAddress || '',
      district: body.district || '',
      upazila: body.upazila || '',
      postCode: body.postCode || '',

      // Contacts
      contactPerson: body.contactPerson || '',
      contactPhone: body.contactPhone || '',
      contactEmail: body.contactEmail || '',
      emergencyContact: body.emergencyContact || '',
      emergencyPhone: body.emergencyPhone || '',

      // Notes
      notes: body.notes || '',

      // Meta
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
      createdBy: body.createdBy || 'unknown_user',
      branchId: body.branchId || 'main_branch',

      // Dates
      commencementDate: body.commencementDate || new Date().toISOString().split('T')[0], // Default to today
      completionDate: body.completionDate || ''
    };

    await loans.insertOne(loanDoc);
    return res.status(201).json({ success: true, id: loanId, loan: loanDoc });
  } catch (error) {
    console.error('Create giving loan error:', error);
    return res.status(500).json({ error: true, message: 'Failed to create giving loan' });
  }
});

// ✅ POST: Create Loan (Receiving) – stores in 'loans' collection and returns generated id
app.post("/loans/receiving", async (req, res) => {
  try {
    const body = req.body || {};
    const firstName = (body.firstName || '').trim();
    const lastName = (body.lastName || '').trim();
    if (!firstName || !lastName) {
      return res.status(400).json({ error: true, message: "firstName and lastName are required" });
    }

    const loanDirection = 'receiving';
    const loanId = await generateLoanId(db, loanDirection);
    const fullName = `${firstName} ${lastName}`.trim();

    const loanDoc = {
      loanId,
      loanDirection, // 'receiving'
      status: 'Pending',

      // Personal
      firstName,
      lastName,
      fullName,
      fatherName: body.fatherName || '',
      motherName: body.motherName || '',
      dateOfBirth: body.dateOfBirth || '',
      gender: body.gender || '',
      maritalStatus: body.maritalStatus || '',
      nidNumber: body.nidNumber || '',
      nidFrontImage: body.nidFrontImage || '',
      nidBackImage: body.nidBackImage || '',
      profilePhoto: body.profilePhoto || '',

      // Address
      presentAddress: body.presentAddress || '',
      permanentAddress: body.permanentAddress || '',
      district: body.district || '',
      upazila: body.upazila || '',
      postCode: body.postCode || '',

      // Business (optional; provided by receiving form)
      businessName: body.businessName || '',
      businessType: body.businessType || '',
      businessAddress: body.businessAddress || '',
      businessRegistration: body.businessRegistration || '',
      businessExperience: body.businessExperience || '',

      // Contacts
      contactPerson: body.contactPerson || '',
      contactPhone: body.contactPhone || '',
      contactEmail: body.contactEmail || '',
      emergencyContact: body.emergencyContact || '',
      emergencyPhone: body.emergencyPhone || '',

      // Notes
      notes: body.notes || '',

      // Meta
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date(),
      createdBy: body.createdBy || 'unknown_user',
      branchId: body.branchId || 'main_branch',

      // Dates
      commencementDate: body.commencementDate || new Date().toISOString().split('T')[0], // Default to today
      completionDate: body.completionDate || ''
    };

    await loans.insertOne(loanDoc);
    return res.status(201).json({ success: true, id: loanId, loan: loanDoc });
  } catch (error) {
    console.error('Create receiving loan error:', error);
    return res.status(500).json({ error: true, message: 'Failed to create receiving loan' });
  }
});

// ✅ GET: List Loans (optional filters: loanDirection, status)
app.get("/loans", async (req, res) => {
  try {
    const { loanDirection, status } = req.query || {};
    const filter = { isActive: { $ne: false } };
    if (loanDirection && typeof loanDirection === 'string') {
      filter.loanDirection = loanDirection.toLowerCase();
    }
    if (status && typeof status === 'string') {
      filter.status = status;
    }

    const results = await loans
      .find(filter)
      .sort({ createdAt: -1 })
      .toArray();

    return res.json({ success: true, loans: results });
  } catch (error) {
    console.error('List loans error:', error);
    return res.status(500).json({ error: true, message: 'Failed to fetch loans' });
  }
});

// ✅ GET: Single Loan by loanId
app.get("/loans/:loanId", async (req, res) => {
  try {
    const { loanId } = req.params;
    const loan = await loans.findOne({ loanId, isActive: { $ne: false } });
    if (!loan) {
      return res.status(404).json({ error: true, message: 'Loan not found' });
    }

    // Compute up-to-date amounts
    let totalAmount = Number(loan.totalAmount || 0) || 0;
    let paidAmount = Number(loan.paidAmount || 0) || 0;
    let totalDue = Number(loan.totalDue || 0) || 0;

    // If amounts are missing, derive from transactions
    if (totalAmount === 0 && paidAmount === 0 && totalDue === 0) {
      try {
        const partyIdOptions = [String(loan.loanId)].filter(Boolean);
        if (loan._id) partyIdOptions.push(String(loan._id));
        const agg = await transactions.aggregate([
          {
            $match: {
              isActive: { $ne: false },
              status: 'completed',
              partyType: 'loan',
              partyId: { $in: partyIdOptions }
            }
          },
          {
            $group: {
              _id: null,
              sumDebit: {
                $sum: {
                  $cond: [{ $eq: ["$transactionType", 'debit'] }, "$amount", 0]
                }
              },
              sumCredit: {
                $sum: {
                  $cond: [{ $eq: ["$transactionType", 'credit'] }, "$amount", 0]
                }
              }
            }
          }
        ]).toArray();
        const rec = agg && agg[0];
        if (rec) {
          totalAmount = Number(rec.sumDebit || 0);
          paidAmount = Number(rec.sumCredit || 0);
        }
      } catch (_) {}
    }

    // Clamp and compute due
    if (!Number.isFinite(totalAmount) || totalAmount < 0) totalAmount = 0;
    if (!Number.isFinite(paidAmount) || paidAmount < 0) paidAmount = 0;
    if (paidAmount > totalAmount) paidAmount = totalAmount;
    totalDue = Math.max(0, totalAmount - paidAmount);

    return res.json({
      success: true,
      loan: {
        ...loan,
        totalAmount,
        paidAmount,
        totalDue
      }
    });
  } catch (error) {
    console.error('Get loan error:', error);
    return res.status(500).json({ error: true, message: 'Failed to fetch loan' });
  }
});

// ✅ PUT: Update Loan by loanId (or _id)
app.put("/loans/:loanId", async (req, res) => {
  try {
    const { loanId } = req.params;
    const body = req.body || {};

    const condition = ObjectId.isValid(loanId)
      ? { $or: [{ loanId: String(loanId) }, { _id: new ObjectId(loanId) }], isActive: { $ne: false } }
      : { loanId: String(loanId), isActive: { $ne: false } };

    const existing = await loans.findOne(condition);
    if (!existing) {
      return res.status(404).json({ error: true, message: "Loan not found" });
    }

    const allowedString = (v) => (v === undefined || v === null) ? undefined : String(v).trim();

    const update = { $set: { updatedAt: new Date() } };

    // Personal
    const firstName = allowedString(body.firstName);
    const lastName = allowedString(body.lastName);
    if (firstName !== undefined) update.$set.firstName = firstName;
    if (lastName !== undefined) update.$set.lastName = lastName;
    // fullName derived if any name changed
    if (firstName !== undefined || lastName !== undefined) {
      const fn = firstName !== undefined ? firstName : existing.firstName || '';
      const ln = lastName !== undefined ? lastName : existing.lastName || '';
      update.$set.fullName = `${fn || ''} ${ln || ''}`.trim();
    }
    const fields = [
      'fatherName','motherName','dateOfBirth','gender','maritalStatus','nidNumber',
      'nidFrontImage','nidBackImage','profilePhoto',
      'presentAddress','permanentAddress','district','upazila','postCode',
      'contactPerson','contactPhone','contactEmail','emergencyContact','emergencyPhone',
      'notes',
      // Receiving/business fields
      'businessName','businessType','businessAddress','businessRegistration','businessExperience',
      // Dates
      'commencementDate', 'completionDate',
      // Meta edits
      'status','branchId','createdBy'
    ];
    for (const key of fields) {
      const val = allowedString(body[key]);
      if (val !== undefined) update.$set[key] = val;
    }

    // Optional: isActive boolean toggle
    if (typeof body.isActive === 'boolean') {
      update.$set.isActive = body.isActive;
    }

    const result = await loans.findOneAndUpdate(
      { _id: existing._id },
      update,
      { returnDocument: 'after' }
    );
    const updated = result && (result.value || result);

    // Compute live totals similar to GET
    let totalAmount = Number(updated.totalAmount || 0) || 0;
    let paidAmount = Number(updated.paidAmount || 0) || 0;
    let totalDue = Number(updated.totalDue || 0) || 0;

    if (totalAmount === 0 && paidAmount === 0 && totalDue === 0) {
      try {
        const partyIdOptions = [String(updated.loanId)].filter(Boolean);
        if (updated._id) partyIdOptions.push(String(updated._id));
        const agg = await transactions.aggregate([
          { $match: { isActive: { $ne: false }, status: 'completed', partyType: 'loan', partyId: { $in: partyIdOptions } } },
          { $group: {
              _id: null,
              sumDebit: { $sum: { $cond: [{ $eq: ["$transactionType", 'debit'] }, "$amount", 0] } },
              sumCredit:{ $sum: { $cond: [{ $eq: ["$transactionType", 'credit'] }, "$amount", 0] } }
          } }
        ]).toArray();
        const rec = agg && agg[0];
        if (rec) {
          totalAmount = Number(rec.sumDebit || 0);
          paidAmount = Number(rec.sumCredit || 0);
        }
      } catch (_) {}
    }

    if (!Number.isFinite(totalAmount) || totalAmount < 0) totalAmount = 0;
    if (!Number.isFinite(paidAmount) || paidAmount < 0) paidAmount = 0;
    if (paidAmount > totalAmount) paidAmount = totalAmount;
    totalDue = Math.max(0, totalAmount - paidAmount);

    return res.json({ success: true, message: "Loan updated successfully", loan: { ...updated, totalAmount, paidAmount, totalDue } });
  } catch (error) {
    console.error('Update loan error:', error);
    return res.status(500).json({ error: true, message: 'Failed to update loan' });
  }
});

// ✅ DELETE: Delete Loan by loanId (soft delete)
app.delete("/loans/:loanId", async (req, res) => {
  try {
    const { loanId } = req.params;

    const condition = ObjectId.isValid(loanId)
      ? { $or: [{ loanId: String(loanId) }, { _id: new ObjectId(loanId) }], isActive: { $ne: false } }
      : { loanId: String(loanId), isActive: { $ne: false } };

    const existing = await loans.findOne(condition);
    if (!existing) {
      return res.status(404).json({ error: true, message: "Loan not found" });
    }

    // Soft delete (set isActive to false)
    const result = await loans.updateOne(
      { _id: existing._id },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: true, message: "Loan not found" });
    }

    return res.json({ success: true, message: "Loan deleted successfully" });
  } catch (error) {
    console.error('Delete loan error:', error);
    return res.status(500).json({ error: true, message: 'Failed to delete loan' });
  }
});

// ✅ GET: Loan dashboard summary (volume + profit/loss)
app.get("/loans/dashboard/summary", async (req, res) => {
  try {
    const { fromDate, toDate, loanDirection, status, branchId } = req.query || {};

    // Build loan filter
    const loanFilter = { isActive: { $ne: false } };
    if (loanDirection) {
      loanFilter.loanDirection = String(loanDirection).toLowerCase();
    }
    if (status) {
      loanFilter.status = status;
    }
    if (branchId) {
      loanFilter.branchId = branchId;
    }
    if (fromDate || toDate) {
      loanFilter.createdAt = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (isNaN(start.getTime())) {
          return res.status(400).json({ success: false, message: 'Invalid fromDate' });
        }
        loanFilter.createdAt.$gte = start;
      }
      if (toDate) {
        const end = new Date(toDate);
        if (isNaN(end.getTime())) {
          return res.status(400).json({ success: false, message: 'Invalid toDate' });
        }
        end.setHours(23, 59, 59, 999);
        loanFilter.createdAt.$lte = end;
      }
    }

    // Build transaction filter (completed loan transactions)
    const txFilter = { isActive: { $ne: false }, status: 'completed', partyType: 'loan' };
    if (branchId) {
      txFilter.branchId = branchId;
    }
    if (fromDate || toDate) {
      txFilter.date = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (!isNaN(start.getTime())) {
          txFilter.date.$gte = start;
        }
      }
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
          txFilter.date.$lte = end;
        }
      }
    }

    // Base totals from loan profiles
    const baseAgg = await loans.aggregate([
      { $match: loanFilter },
      {
        $group: {
          _id: null,
          totalLoans: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          totalDue: { $sum: { $ifNull: ["$totalDue", 0] } },
          active: { $sum: { $cond: [{ $eq: ["$status", "Active"] }, 1, 0] } },
          pending: { $sum: { $cond: [{ $eq: ["$status", "Pending"] }, 1, 0] } },
          closed: { $sum: { $cond: [{ $eq: ["$status", "Closed"] }, 1, 0] } },
          rejected: { $sum: { $cond: [{ $eq: ["$status", "Rejected"] }, 1, 0] } }
        }
      }
    ]).toArray();
    const base = baseAgg[0] || {
      totalLoans: 0,
      totalAmount: 0,
      paidAmount: 0,
      totalDue: 0,
      active: 0,
      pending: 0,
      closed: 0,
      rejected: 0
    };

    // Calculate actual cash flow from transactions
    // 1. Total Principal Disbursed (We gave loan) = Debit transactions on 'giving' loans
    // 2. Total Principal Repaid to us (We received back) = Credit transactions on 'giving' loans
    // 3. Total Principal Received (We took loan) = Credit transactions on 'receiving' loans
    // 4. Total Principal Repaid by us (We paid back) = Debit transactions on 'receiving' loans
    
    // Profit calculation:
    // For 'giving' loans: Profit = (Total Repaid by borrower) - (Principal Disbursed) [Simple view, but actually profit comes from interest/extra]
    // Note: The system currently tracks totalAmount and paidAmount.
    // Let's rely on transaction summation for accurate cash flow.

    const cashFlowAgg = await transactions.aggregate([
      { $match: txFilter },
      {
        $lookup: {
          from: "loans",
          let: { pid: "$partyId" },
          pipeline: [
            {
              $match: {
                $expr: {
                  $or: [
                    { $eq: ["$loanId", "$$pid"] },
                    { $eq: [{ $toString: "$_id" }, "$$pid"] }
                  ]
                }
              }
            },
            { $project: { loanDirection: 1 } }
          ],
          as: "loan"
        }
      },
      { $unwind: { path: "$loan", preserveNullAndEmptyArrays: true } },
      {
        $group: {
          _id: "$loan.loanDirection",
          totalDebit: { $sum: { $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0] } },
          totalCredit: { $sum: { $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0] } }
        }
      }
    ]).toArray();

    let givingDisbursed = 0; // We gave out (Debit)
    let givingRepaid = 0;    // We got back (Credit)
    let receivingTaken = 0;  // We took loan (Credit)
    let receivingRepaid = 0; // We paid back (Debit)

    cashFlowAgg.forEach(row => {
      if (row._id === 'giving') {
        givingDisbursed = row.totalDebit || 0;
        givingRepaid = row.totalCredit || 0;
      } else if (row._id === 'receiving') {
        receivingTaken = row.totalCredit || 0;
        receivingRepaid = row.totalDebit || 0;
      }
    });

    // Net Profit/Loss logic:
    // Profit from Giving Loans = Repaid - Disbursed (if Repaid > Disbursed, else it's just recovery so far)
    // Actually, simple cashflow:
    // Net Cash In = (Giving Repaid + Receiving Taken)
    // Net Cash Out = (Giving Disbursed + Receiving Repaid)
    // Net Cash Flow = Net Cash In - Net Cash Out
    
    const totalCashIn = givingRepaid + receivingTaken;
    const totalCashOut = givingDisbursed + receivingRepaid;
    const netCashFlow = totalCashIn - totalCashOut;

    // Breakdown by loan direction
    const directionBreakdown = await loans.aggregate([
      { $match: loanFilter },
      {
        $group: {
          _id: "$loanDirection",
          count: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          totalDue: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    // Status breakdown
    const statusBreakdown = await loans.aggregate([
      { $match: loanFilter },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          totalDue: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    // Transaction totals (profit/loss computed from cashflow)
    const txTotalsAgg = await transactions.aggregate([
      { $match: txFilter },
      {
        $group: {
          _id: null,
          totalTransactions: { $sum: 1 },
          totalDebit: { $sum: { $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0] } },
          totalCredit: { $sum: { $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0] } }
        }
      }
    ]).toArray();
    const txTotals = txTotalsAgg[0] || {
      totalTransactions: 0,
      totalDebit: 0,
      totalCredit: 0
    };

    // Transactions grouped by loan direction (via lookup)
    const txByDirectionRaw = await transactions.aggregate([
      { $match: txFilter },
      {
        $lookup: {
          from: "loans",
          let: { pid: "$partyId" },
          pipeline: [
            {
              $match: {
                $expr: {
                  $or: [
                    { $eq: ["$loanId", "$$pid"] },
                    { $eq: [{ $toString: "$_id" }, "$$pid"] }
                  ]
                }
              }
            },
            { $project: { loanDirection: 1 } }
          ],
          as: "loan"
        }
      },
      { $unwind: { path: "$loan", preserveNullAndEmptyArrays: true } },
      {
        $group: {
          _id: "$loan.loanDirection",
          count: { $sum: 1 },
          debit: { $sum: { $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0] } },
          credit: { $sum: { $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0] } }
        }
      }
    ]).toArray();

    const txByDirection = txByDirectionRaw.map((row) => ({
      loanDirection: row._id || 'unknown',
      count: row.count || 0,
      totalDebit: Number(row.debit || 0),
      totalCredit: Number(row.credit || 0),
      netCashflow: Number(row.credit || 0) - Number(row.debit || 0)
    }));

    const response = {
      totals: {
        totalLoans: base.totalLoans,
        active: base.active,
        pending: base.pending,
        closed: base.closed,
        rejected: base.rejected
      },
      financial: {
        totalAmount: Number(base.totalAmount || 0),
        paidAmount: Number(base.paidAmount || 0),
        totalDue: Number(base.totalDue || 0),
        // Profit/Loss based on Net Cash Flow
        netCashFlow: netCashFlow,
        // Detailed cash flow
        cashIn: totalCashIn,
        cashOut: totalCashOut,
        // Breakdown
        givingDisbursed,
        givingRepaid,
        receivingTaken,
        receivingRepaid
      },
      directionBreakdown: directionBreakdown.map((d) => ({
        loanDirection: d._id || 'unknown',
        count: d.count || 0,
        totalAmount: Number(d.totalAmount || 0),
        paidAmount: Number(d.paidAmount || 0),
        totalDue: Number(d.totalDue || 0)
      })),
      statusBreakdown: statusBreakdown.map((s) => ({
        status: s._id || 'unknown',
        count: s.count || 0,
        totalAmount: Number(s.totalAmount || 0),
        totalDue: Number(s.totalDue || 0)
      })),
      transactions: {
        totalTransactions: txTotals.totalTransactions || 0,
        totalDebit: Number(txTotals.totalDebit || 0),
        totalCredit: Number(txTotals.totalCredit || 0),
        netCashflow: Number(txTotals.totalCredit || 0) - Number(txTotals.totalDebit || 0),
        byDirection: txByDirection
      }
    };

    return res.json({ success: true, data: response });
  } catch (error) {
    console.error('Loan dashboard summary error:', error);
    return res.status(500).json({ success: false, message: 'Failed to fetch loan dashboard summary' });
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

// Helper: Generate unique Air Ticket ID
const generateAirTicketId = async (db) => {
  const counterCollection = db.collection("counters");
  
  // Get current date in DDMMYY format
  const today = new Date();
  const dateStr = String(today.getDate()).padStart(2, '0') +
    String(today.getMonth() + 1).padStart(2, '0') +
    today.getFullYear().toString().slice(-2);
  
  // Create counter key for air tickets with date (resets daily)
  const counterKey = `air_ticket_${dateStr}`;
  
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
  
  // Format: TKT + DDMMYY + 0001 (e.g., TKT1301250001)
  const serial = String(newSequence).padStart(4, '0');
  
  return `TKT${dateStr}${serial}`;
};

// Helper: Generate unique Air Ticketing Agent ID
const generateAirAgentId = async (db) => {
  const counterCollection = db.collection("counters");
  
  // Create counter key for air ticketing agent
  const counterKey = `air_agent`;
  
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
  
  // Format: AT + 00001 (e.g., AT00001)
  const serial = String(newSequence).padStart(5, '0');
  
  return `AT${serial}`;
};

// ==================== AIR TICKETING AGENT ROUTES ====================

// ✅ POST: Create new Air Ticketing Agent
app.post("/api/air-ticketing/agents", async (req, res) => {
  try {
    const {
      name, // Trade Name
      personalName,
      email,
      mobile,
      address,
      city,
      state,
      zipCode,
      country,
      nid,
      passport,
      tradeLicense,
      tinNumber
    } = req.body;

    // Validation
    if (!name || !name.trim()) {
      return res.status(400).json({
        success: false,
        message: 'Trade Name is required'
      });
    }

    if (!email || !email.trim()) {
      return res.status(400).json({
        success: false,
        message: 'Email is required'
      });
    }

    if (!mobile || !mobile.trim()) {
      return res.status(400).json({
        success: false,
        message: 'Mobile number is required'
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email.trim())) {
      return res.status(400).json({
        success: false,
        message: 'Please provide a valid email address'
      });
    }

    // Validate mobile format
    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
    if (!phoneRegex.test(mobile.trim())) {
      return res.status(400).json({
        success: false,
        message: 'Please provide a valid mobile number'
      });
    }

    // Check if email already exists
    const existingAgentByEmail = await agents.findOne({
      email: email.trim().toLowerCase(),
      isActive: { $ne: false }
    });

    if (existingAgentByEmail) {
      return res.status(409).json({
        success: false,
        message: 'An agent with this email already exists'
      });
    }

    // Check if mobile already exists
    const existingAgentByMobile = await agents.findOne({
      mobile: mobile.trim(),
      isActive: { $ne: false }
    });

    if (existingAgentByMobile) {
      return res.status(409).json({
        success: false,
        message: 'An agent with this mobile number already exists'
      });
    }

    // Generate agent ID
    const agentId = await generateAirAgentId(db);

    // Create agent document
    const agentData = {
      agentId,
      agentType: 'air-ticketing', // To distinguish from haj-umrah agents
      name: name.trim(), // Trade Name
      personalName: personalName ? personalName.trim() : null,
      email: email.trim().toLowerCase(),
      mobile: mobile.trim(),
      address: address ? address.trim() : null,
      city: city ? city.trim() : null,
      state: state ? state.trim() : null,
      zipCode: zipCode ? zipCode.trim() : null,
      country: country || 'Bangladesh',
      // KYC Information
      nid: nid ? nid.trim() : null,
      passport: passport ? passport.trim() : null,
      tradeLicense: tradeLicense ? tradeLicense.trim() : null,
      tinNumber: tinNumber ? tinNumber.trim() : null,
      // Status
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Insert agent
    const result = await agents.insertOne(agentData);

    // Fetch created agent
    const createdAgent = await agents.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: 'Air Ticketing Agent created successfully',
      agent: createdAgent
    });

  } catch (error) {
    console.error('Create air ticketing agent error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create air ticketing agent',
      error: error.message
    });
  }
});

// ✅ GET: Get all Air Ticketing Agents with filters and pagination
app.get("/api/air-ticketing/agents", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 20,
      q, // search query
      country
    } = req.query || {};

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);
    const skip = (pageNum - 1) * limitNum;

    // Build query - only air ticketing agents
    const query = {
      agentType: 'air-ticketing',
      isActive: { $ne: false }
    };

    // Search filter
    if (q) {
      const searchTerm = String(q).trim();
      query.$or = [
        { name: { $regex: searchTerm, $options: 'i' } },
        { personalName: { $regex: searchTerm, $options: 'i' } },
        { email: { $regex: searchTerm, $options: 'i' } },
        { mobile: { $regex: searchTerm, $options: 'i' } },
        { agentId: { $regex: searchTerm, $options: 'i' } },
        { nid: { $regex: searchTerm, $options: 'i' } },
        { passport: { $regex: searchTerm, $options: 'i' } },
        { tradeLicense: { $regex: searchTerm, $options: 'i' } }
      ];
    }

    // Country filter
    if (country) {
      query.country = country;
    }

    // Get total count
    const total = await agents.countDocuments(query);

    // Get agents
    const agentsList = await agents
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data: agentsList,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get air ticketing agents error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch air ticketing agents',
      error: error.message
    });
  }
});

// ✅ GET: Get single Air Ticketing Agent by ID
app.get("/api/air-ticketing/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { agentId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      agentType: 'air-ticketing',
      isActive: { $ne: false }
    };

    const agent = await agents.findOne(query);

    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Air Ticketing Agent not found'
      });
    }

    res.json({
      success: true,
      agent
    });

  } catch (error) {
    console.error('Get air ticketing agent error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch air ticketing agent',
      error: error.message
    });
  }
});

// ✅ PUT: Update Air Ticketing Agent by ID
app.put("/api/air-ticketing/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const {
      name,
      personalName,
      email,
      mobile,
      address,
      city,
      state,
      zipCode,
      country,
      nid,
      passport,
      tradeLicense,
      tinNumber
    } = req.body;

    // Find agent
    const query = {
      $or: [
        { agentId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      agentType: 'air-ticketing',
      isActive: { $ne: false }
    };

    const existingAgent = await agents.findOne(query);

    if (!existingAgent) {
      return res.status(404).json({
        success: false,
        message: 'Air Ticketing Agent not found'
      });
    }

    // Build update object
    const updateFields = {
      updatedAt: new Date()
    };

    // Update allowed fields
    if (name !== undefined) {
      if (!name || !name.trim()) {
        return res.status(400).json({
          success: false,
          message: 'Trade Name cannot be empty'
        });
      }
      updateFields.name = name.trim();
    }

    if (personalName !== undefined) {
      updateFields.personalName = personalName ? personalName.trim() : null;
    }

    if (email !== undefined) {
      if (!email || !email.trim()) {
        return res.status(400).json({
          success: false,
          message: 'Email cannot be empty'
        });
      }
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(email.trim())) {
        return res.status(400).json({
          success: false,
          message: 'Please provide a valid email address'
        });
      }
      // Check if email already exists (excluding current agent)
      const emailExists = await agents.findOne({
        email: email.trim().toLowerCase(),
        _id: { $ne: existingAgent._id },
        isActive: { $ne: false }
      });
      if (emailExists) {
        return res.status(409).json({
          success: false,
          message: 'An agent with this email already exists'
        });
      }
      updateFields.email = email.trim().toLowerCase();
    }

    if (mobile !== undefined) {
      if (!mobile || !mobile.trim()) {
        return res.status(400).json({
          success: false,
          message: 'Mobile number cannot be empty'
        });
      }
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(mobile.trim())) {
        return res.status(400).json({
          success: false,
          message: 'Please provide a valid mobile number'
        });
      }
      // Check if mobile already exists (excluding current agent)
      const mobileExists = await agents.findOne({
        mobile: mobile.trim(),
        _id: { $ne: existingAgent._id },
        isActive: { $ne: false }
      });
      if (mobileExists) {
        return res.status(409).json({
          success: false,
          message: 'An agent with this mobile number already exists'
        });
      }
      updateFields.mobile = mobile.trim();
    }

    if (address !== undefined) {
      updateFields.address = address ? address.trim() : null;
    }

    if (city !== undefined) {
      updateFields.city = city ? city.trim() : null;
    }

    if (state !== undefined) {
      updateFields.state = state ? state.trim() : null;
    }

    if (zipCode !== undefined) {
      updateFields.zipCode = zipCode ? zipCode.trim() : null;
    }

    if (country !== undefined) {
      updateFields.country = country || 'Bangladesh';
    }

    if (nid !== undefined) {
      updateFields.nid = nid ? nid.trim() : null;
    }

    if (passport !== undefined) {
      updateFields.passport = passport ? passport.trim() : null;
    }

    if (tradeLicense !== undefined) {
      updateFields.tradeLicense = tradeLicense ? tradeLicense.trim() : null;
    }

    if (tinNumber !== undefined) {
      updateFields.tinNumber = tinNumber ? tinNumber.trim() : null;
    }

    // Update agent
    await agents.updateOne(
      { _id: existingAgent._id },
      { $set: updateFields }
    );

    // Fetch updated agent
    const updatedAgent = await agents.findOne({ _id: existingAgent._id });

    res.json({
      success: true,
      message: 'Air Ticketing Agent updated successfully',
      agent: updatedAgent
    });

  } catch (error) {
    console.error('Update air ticketing agent error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update air ticketing agent',
      error: error.message
    });
  }
});

// ✅ DELETE: Delete Air Ticketing Agent by ID (soft delete)
app.delete("/api/air-ticketing/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { agentId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      agentType: 'air-ticketing',
      isActive: { $ne: false }
    };

    const agent = await agents.findOne(query);

    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Air Ticketing Agent not found'
      });
    }

    // Soft delete
    await agents.updateOne(
      { _id: agent._id },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: 'Air Ticketing Agent deleted successfully'
    });

  } catch (error) {
    console.error('Delete air ticketing agent error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete air ticketing agent',
      error: error.message
    });
  }
});

// ==================== AIRLINE ROUTES ====================

// Helper: Generate unique Airline ID
const generateAirlineId = async (db) => {
  const counterCollection = db.collection("counters");
  const counterKey = `airline`;
  let counter = await counterCollection.findOne({ counterKey });
  if (!counter) {
    await counterCollection.insertOne({ counterKey, sequence: 0 });
    counter = { sequence: 0 };
  }
  const newSequence = counter.sequence + 1;
  await counterCollection.updateOne({ counterKey }, { $set: { sequence: newSequence } });
  const serial = String(newSequence).padStart(5, '0');
  return `AL${serial}`;
};

// ✅ POST: Create new Airline
app.post("/api/air-ticketing/airlines", async (req, res) => {
  try {
    const { name, code, country, headquarters, phone, email, website, established, status, routes, fleet, logo } = req.body;

    if (!name || !name.trim()) {
      return res.status(400).json({ success: false, message: 'Airline name is required' });
    }

    if (!code || !code.trim()) {
      return res.status(400).json({ success: false, message: 'Airline code is required' });
    }

    // Check if code already exists
    const existingCode = await airlines.findOne({
      code: code.trim().toUpperCase(),
      isActive: { $ne: false }
    });

    if (existingCode) {
      return res.status(409).json({ success: false, message: 'Airline code already exists' });
    }

    const airlineId = await generateAirlineId(db);

    const airlineData = {
      airlineId,
      name: name.trim(),
      code: code.trim().toUpperCase(),
      country: country || null,
      headquarters: headquarters || null,
      phone: phone || null,
      email: email || null,
      website: website || null,
      established: established || null,
      status: status || 'Active',
      routes: routes ? parseInt(routes) : 0,
      fleet: fleet ? parseInt(fleet) : 0,
      logo: logo || null,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await airlines.insertOne(airlineData);
    const createdAirline = await airlines.findOne({ _id: result.insertedId });

    res.status(201).json({
      success: true,
      message: 'Airline created successfully',
      airline: createdAirline
    });

  } catch (error) {
    console.error('Create airline error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create airline',
      error: error.message
    });
  }
});

// ✅ GET: Get all Airlines with filters and pagination
app.get("/api/air-ticketing/airlines", async (req, res) => {
  try {
    const { page = 1, limit = 20, q, status } = req.query || {};
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);
    const skip = (pageNum - 1) * limitNum;

    const query = { isActive: { $ne: false } };

    if (status && status !== 'All') {
      query.status = status;
    }

    if (q) {
      const searchTerm = String(q).trim();
      query.$or = [
        { name: { $regex: searchTerm, $options: 'i' } },
        { code: { $regex: searchTerm, $options: 'i' } },
        { country: { $regex: searchTerm, $options: 'i' } },
        { headquarters: { $regex: searchTerm, $options: 'i' } }
      ];
    }

    const total = await airlines.countDocuments(query);
    const airlinesList = await airlines
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data: airlinesList,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get airlines error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch airlines',
      error: error.message
    });
  }
});

// ✅ GET: Get single Airline by ID
app.get("/api/air-ticketing/airlines/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const query = {
      $or: [
        { airlineId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const airline = await airlines.findOne(query);

    if (!airline) {
      return res.status(404).json({
        success: false,
        message: 'Airline not found'
      });
    }

    res.json({
      success: true,
      airline
    });

  } catch (error) {
    console.error('Get airline error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch airline',
      error: error.message
    });
  }
});

// ✅ PUT: Update Airline by ID
app.put("/api/air-ticketing/airlines/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const { name, code, country, headquarters, phone, email, website, established, status, routes, fleet, logo } = req.body;

    const query = {
      $or: [
        { airlineId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const existingAirline = await airlines.findOne(query);

    if (!existingAirline) {
      return res.status(404).json({
        success: false,
        message: 'Airline not found'
      });
    }

    const updateFields = { updatedAt: new Date() };

    if (name !== undefined) {
      if (!name || !name.trim()) {
        return res.status(400).json({ success: false, message: 'Airline name cannot be empty' });
      }
      updateFields.name = name.trim();
    }

    if (code !== undefined) {
      if (!code || !code.trim()) {
        return res.status(400).json({ success: false, message: 'Airline code cannot be empty' });
      }
      const codeExists = await airlines.findOne({
        code: code.trim().toUpperCase(),
        _id: { $ne: existingAirline._id },
        isActive: { $ne: false }
      });
      if (codeExists) {
        return res.status(409).json({ success: false, message: 'Airline code already exists' });
      }
      updateFields.code = code.trim().toUpperCase();
    }

    if (country !== undefined) updateFields.country = country || null;
    if (headquarters !== undefined) updateFields.headquarters = headquarters || null;
    if (phone !== undefined) updateFields.phone = phone || null;
    if (email !== undefined) updateFields.email = email || null;
    if (website !== undefined) updateFields.website = website || null;
    if (established !== undefined) updateFields.established = established || null;
    if (status !== undefined) updateFields.status = status || 'Active';
    if (routes !== undefined) updateFields.routes = routes ? parseInt(routes) : 0;
    if (fleet !== undefined) updateFields.fleet = fleet ? parseInt(fleet) : 0;
    if (logo !== undefined) updateFields.logo = logo || null;

    await airlines.updateOne({ _id: existingAirline._id }, { $set: updateFields });
    const updatedAirline = await airlines.findOne({ _id: existingAirline._id });

    res.json({
      success: true,
      message: 'Airline updated successfully',
      airline: updatedAirline
    });

  } catch (error) {
    console.error('Update airline error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update airline',
      error: error.message
    });
  }
});

// ✅ DELETE: Delete Airline by ID (soft delete)
app.delete("/api/air-ticketing/airlines/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const query = {
      $or: [
        { airlineId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const airline = await airlines.findOne(query);

    if (!airline) {
      return res.status(404).json({
        success: false,
        message: 'Airline not found'
      });
    }

    await airlines.updateOne(
      { _id: airline._id },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    res.json({
      success: true,
      message: 'Airline deleted successfully'
    });

  } catch (error) {
    console.error('Delete airline error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete airline',
      error: error.message
    });
  }
});

// ==================== AIR TICKETING TICKET ROUTES ====================

// ✅ POST: Create new Air Ticket
app.post("/api/air-ticketing/tickets", async (req, res) => {
  try {
    const ticketData = req.body;

    // Generate unique ticket ID automatically
    const ticketId = await generateAirTicketId(db);

    // Validate required fields
    if (!ticketData.customerId) {
      return res.status(400).json({
        success: false,
        message: 'Customer ID is required'
      });
    }

    if (!ticketData.bookingId) {
      return res.status(400).json({
        success: false,
        message: 'Booking ID is required (must be provided manually)'
      });
    }

    if (!ticketData.airline) {
      return res.status(400).json({
        success: false,
        message: 'Airline is required'
      });
    }

    if (!ticketData.date) {
      return res.status(400).json({
        success: false,
        message: 'Selling date is required'
      });
    }

    // Validate trip type specific fields
    if (ticketData.tripType === 'multicity') {
      if (!ticketData.segments || ticketData.segments.length < 2) {
        return res.status(400).json({
          success: false,
          message: 'At least 2 segments are required for multicity trip'
        });
      }
      // Validate each segment
      for (let i = 0; i < ticketData.segments.length; i++) {
        const seg = ticketData.segments[i];
        if (!seg.origin || !seg.destination || !seg.date) {
          return res.status(400).json({
            success: false,
            message: `Segment ${i + 1} must have origin, destination, and date`
          });
        }
      }
    } else {
      if (!ticketData.origin || !ticketData.destination || !ticketData.flightDate) {
        return res.status(400).json({
          success: false,
          message: 'Origin, destination, and flight date are required'
        });
      }
      if (ticketData.tripType === 'roundtrip' && !ticketData.returnDate) {
        return res.status(400).json({
          success: false,
          message: 'Return date is required for round trip'
        });
      }
    }

    // Verify customer exists in airCustomers collection
    const customer = await airCustomers.findOne({
      $or: [
        { customerId: ticketData.customerId },
        { _id: ObjectId.isValid(ticketData.customerId) ? new ObjectId(ticketData.customerId) : null }
      ],
      isActive: { $ne: false }
    });

    if (!customer) {
      return res.status(404).json({
        success: false,
        message: 'Customer not found'
      });
    }

    // Verify agent exists if agentId is provided
    if (ticketData.agentId) {
      const agent = await agents.findOne({
        $or: [
          { agentId: ticketData.agentId },
          { _id: ObjectId.isValid(ticketData.agentId) ? new ObjectId(ticketData.agentId) : null }
        ],
        agentType: 'air-ticketing',
        isActive: { $ne: false }
      });

      if (!agent) {
        return res.status(404).json({
          success: false,
          message: 'Agent not found'
        });
      }
    }

    // Prepare ticket document
    const ticketDoc = {
      // Unique ticket ID (auto-generated)
      ticketId: ticketId,
      
      // Customer information
      customerId: ticketData.customerId,
      customerName: ticketData.customerName || customer.name,
      customerPhone: ticketData.customerPhone || customer.mobile,

      // Booking information
      tripType: ticketData.tripType || 'oneway',
      flightType: ticketData.flightType || 'domestic',
      date: new Date(ticketData.date),
      bookingId: ticketData.bookingId, // Manual booking ID
      gdsPnr: ticketData.gdsPnr || '',
      airlinePnr: ticketData.airlinePnr || '',
      airline: ticketData.airline,
      status: ticketData.status || 'pending',

      // Route information
      origin: ticketData.origin || '',
      destination: ticketData.destination || '',
      flightDate: ticketData.flightDate ? new Date(ticketData.flightDate) : null,
      returnDate: ticketData.returnDate ? new Date(ticketData.returnDate) : null,
      segments: ticketData.segments || [],

      // Agent information
      agent: ticketData.agent || '',
      agentId: ticketData.agentId || '',
      purposeType: ticketData.purposeType || '',

      // Passenger information
      adultCount: parseInt(ticketData.adultCount) || 0,
      childCount: parseInt(ticketData.childCount) || 0,
      infantCount: parseInt(ticketData.infantCount) || 0,

      // Customer financial information
      customerDeal: parseFloat(ticketData.customerDeal) || 0,
      customerPaid: parseFloat(ticketData.customerPaid) || 0,
      customerDue: parseFloat(ticketData.customerDue) || 0,
      dueDate: ticketData.dueDate ? new Date(ticketData.dueDate) : null,

      // Vendor amount breakdown
      baseFare: parseFloat(ticketData.baseFare) || 0,
      taxBD: parseFloat(ticketData.taxBD) || 0,
      e5: parseFloat(ticketData.e5) || 0,
      e7: parseFloat(ticketData.e7) || 0,
      g8: parseFloat(ticketData.g8) || 0,
      ow: parseFloat(ticketData.ow) || 0,
      p7: parseFloat(ticketData.p7) || 0,
      p8: parseFloat(ticketData.p8) || 0,
      ts: parseFloat(ticketData.ts) || 0,
      ut: parseFloat(ticketData.ut) || 0,
      yq: parseFloat(ticketData.yq) || 0,
      taxes: parseFloat(ticketData.taxes) || 0,
      totalTaxes: parseFloat(ticketData.totalTaxes) || 0,
      ait: parseFloat(ticketData.ait) || 0,

      // Commission and charges
      commissionRate: parseFloat(ticketData.commissionRate) || 0,
      plb: parseFloat(ticketData.plb) || 0,
      salmaAirServiceCharge: parseFloat(ticketData.salmaAirServiceCharge) || 0,
      vendorServiceCharge: parseFloat(ticketData.vendorServiceCharge) || 0,

      // Vendor financial information
      vendorAmount: parseFloat(ticketData.vendorAmount) || 0,
      vendorPaidFh: parseFloat(ticketData.vendorPaidFh) || 0,
      vendorDue: parseFloat(ticketData.vendorDue) || 0,
      profit: parseFloat(ticketData.profit) || 0,

      // Additional information
      segmentCount: parseInt(ticketData.segmentCount) || (ticketData.tripType === 'multicity' ? (ticketData.segments?.length || 0) : 1),
      flownSegment: ticketData.flownSegment || false,

      // Metadata
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Start transaction for atomic operations
    let session = null;
    let insertedTicketObjectId = null;
    try {
      session = client.startSession();
      await session.withTransaction(async () => {
        // Insert ticket
        const result = await tickets.insertOne(ticketDoc, { session });
        insertedTicketObjectId = result.insertedId;

        // Update customer's financial information in airCustomers collection
        const customerDeal = parseFloat(ticketData.customerDeal) || 0;
        const customerPaid = parseFloat(ticketData.customerPaid) || 0;
        const customerDue = parseFloat(ticketData.customerDue) || 0;

        // Calculate current customer totals
        const currentTotalAmount = (customer.totalAmount || 0) + customerDeal;
        const currentPaidAmount = (customer.paidAmount || 0) + customerPaid;
        const currentTotalDue = (customer.totalDue || 0) + customerDue;

        // Update customer with new financial totals
        await airCustomers.updateOne(
          { _id: customer._id },
          {
            $set: {
              totalAmount: Math.max(0, currentTotalAmount),
              paidAmount: Math.max(0, currentPaidAmount),
              totalDue: Math.max(0, currentTotalDue),
              updatedAt: new Date()
            }
          },
          { session }
        );
      });

      // Return created ticket
      const createdTicket = await tickets.findOne({ _id: insertedTicketObjectId });

      res.status(201).json({
        success: true,
        message: 'Ticket created successfully',
        ticket: createdTicket
      });
    } finally {
      await session.endSession();
    }

  } catch (error) {
    console.error('Create ticket error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to create ticket',
      error: error.message
    });
  }
});

// ✅ GET: Get all Air Tickets with filters and pagination
app.get("/api/air-ticketing/tickets", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 20,
      q, // search query
      customerId,
      agentId,
      status,
      flightType,
      tripType,
      airline,
      dateFrom,
      dateTo,
      bookingId
    } = req.query || {};

    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);
    const skip = (pageNum - 1) * limitNum;

    // Build query
    const query = {
      isActive: { $ne: false }
    };

    // Search filter
    if (q) {
      const searchTerm = String(q).trim();
      query.$or = [
        { bookingId: { $regex: searchTerm, $options: 'i' } },
        { gdsPnr: { $regex: searchTerm, $options: 'i' } },
        { airlinePnr: { $regex: searchTerm, $options: 'i' } },
        { customerName: { $regex: searchTerm, $options: 'i' } },
        { customerPhone: { $regex: searchTerm, $options: 'i' } },
        { airline: { $regex: searchTerm, $options: 'i' } },
        { agent: { $regex: searchTerm, $options: 'i' } }
      ];
    }

    // Other filters
    if (customerId) {
      query.customerId = customerId;
    }
    if (agentId) {
      query.agentId = agentId;
    }
    if (status) {
      query.status = status;
    }
    if (flightType) {
      query.flightType = flightType;
    }
    if (tripType) {
      query.tripType = tripType;
    }
    if (airline) {
      query.airline = { $regex: airline, $options: 'i' };
    }
    if (bookingId) {
      query.bookingId = { $regex: bookingId, $options: 'i' };
    }
    if (dateFrom || dateTo) {
      query.date = {};
      if (dateFrom) {
        query.date.$gte = new Date(dateFrom);
      }
      if (dateTo) {
        const toDate = new Date(dateTo);
        toDate.setHours(23, 59, 59, 999);
        query.date.$lte = toDate;
      }
    }

    // Get total count
    const total = await tickets.countDocuments(query);

    // Get tickets
    const ticketsList = await tickets
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data: ticketsList,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get tickets error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch tickets',
      error: error.message
    });
  }
});

// ✅ GET: Air Ticketing dashboard summary (profit/loss, trends)
app.get("/api/air-ticketing/dashboard/summary", async (req, res) => {
  try {
    const {
      dateFrom,
      dateTo,
      airline,
      agentId,
      status,
      flightType,
      tripType
    } = req.query || {};

    // Build filter for the aggregation
    const match = { isActive: { $ne: false } };

    // Selling/issue date filter
    if (dateFrom || dateTo) {
      match.date = {};
      if (dateFrom) {
        const start = new Date(dateFrom);
        if (isNaN(start.getTime())) {
          return res.status(400).json({
            success: false,
            message: 'Invalid dateFrom value'
          });
        }
        match.date.$gte = start;
      }
      if (dateTo) {
        const end = new Date(dateTo);
        if (isNaN(end.getTime())) {
          return res.status(400).json({
            success: false,
            message: 'Invalid dateTo value'
          });
        }
        end.setHours(23, 59, 59, 999);
        match.date.$lte = end;
      }
    }

    if (airline) {
      match.airline = { $regex: airline, $options: 'i' };
    }
    if (agentId) {
      match.agentId = agentId;
    }
    if (status) {
      match.status = status;
    }
    if (flightType) {
      match.flightType = flightType;
    }
    if (tripType) {
      match.tripType = tripType;
    }

    // Common profit expression with fallback when profit field is missing
    const profitExpression = {
      $ifNull: [
        "$profit",
        {
          $subtract: [
            { $ifNull: ["$customerDeal", 0] },
            { $ifNull: ["$vendorAmount", 0] }
          ]
        }
      ]
    };

    // Base financial and volume stats
    const baseStatsAgg = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: null,
          totalTickets: { $sum: 1 },
          totalSegments: { $sum: { $ifNull: ["$segmentCount", 0] } },
          adults: { $sum: { $ifNull: ["$adultCount", 0] } },
          children: { $sum: { $ifNull: ["$childCount", 0] } },
          infants: { $sum: { $ifNull: ["$infantCount", 0] } },
          customerDeal: { $sum: { $ifNull: ["$customerDeal", 0] } },
          customerPaid: { $sum: { $ifNull: ["$customerPaid", 0] } },
          customerDue: { $sum: { $ifNull: ["$customerDue", 0] } },
          vendorAmount: { $sum: { $ifNull: ["$vendorAmount", 0] } },
          vendorPaid: { $sum: { $ifNull: ["$vendorPaidFh", 0] } },
          vendorDue: { $sum: { $ifNull: ["$vendorDue", 0] } },
          profit: { $sum: profitExpression }
        }
      }
    ]).toArray();

    const baseStats = baseStatsAgg[0] || {
      totalTickets: 0,
      totalSegments: 0,
      adults: 0,
      children: 0,
      infants: 0,
      customerDeal: 0,
      customerPaid: 0,
      customerDue: 0,
      vendorAmount: 0,
      vendorPaid: 0,
      vendorDue: 0,
      profit: 0
    };

    // Status breakdown
    const statusBreakdownRaw = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 },
          revenue: { $sum: { $ifNull: ["$customerDeal", 0] } },
          profit: { $sum: profitExpression }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    const statusBreakdown = statusBreakdownRaw.reduce((acc, item) => {
      const key = item._id || 'unknown';
      acc[key] = {
        count: item.count,
        revenue: Number((item.revenue || 0).toFixed(2)),
        profit: Number((item.profit || 0).toFixed(2))
      };
      return acc;
    }, {});

    // Flight type breakdown
    const flightTypeBreakdown = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: "$flightType",
          count: { $sum: 1 },
          revenue: { $sum: { $ifNull: ["$customerDeal", 0] } },
          profit: { $sum: profitExpression }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    // Trip type breakdown
    const tripTypeBreakdown = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: "$tripType",
          count: { $sum: 1 },
          revenue: { $sum: { $ifNull: ["$customerDeal", 0] } },
          profit: { $sum: profitExpression }
        }
      },
      { $sort: { count: -1 } }
    ]).toArray();

    // Top airlines
    const topAirlines = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: "$airline",
          count: { $sum: 1 },
          revenue: { $sum: { $ifNull: ["$customerDeal", 0] } },
          profit: { $sum: profitExpression }
        }
      },
      { $sort: { revenue: -1, count: -1 } },
      { $limit: 5 }
    ]).toArray();

    // Top agents
    const topAgents = await tickets.aggregate([
      { $match: match },
      {
        $group: {
          _id: { agentId: "$agentId", agent: "$agent" },
          count: { $sum: 1 },
          revenue: { $sum: { $ifNull: ["$customerDeal", 0] } },
          profit: { $sum: profitExpression }
        }
      },
      { $sort: { revenue: -1, count: -1 } },
      { $limit: 5 }
    ]).toArray();

    // Recent tickets for quick view
    const recentTickets = await tickets
      .find(match)
      .sort({ createdAt: -1 })
      .limit(5)
      .toArray();

    const normalizedRecent = recentTickets.map(t => {
      const profitValue = typeof t.profit === 'number'
        ? t.profit
        : ((parseFloat(t.customerDeal) || 0) - (parseFloat(t.vendorAmount) || 0));

      return {
        id: t._id,
        bookingId: t.bookingId,
        airline: t.airline,
        agent: t.agent || '',
        customerName: t.customerName,
        flightType: t.flightType,
        tripType: t.tripType,
        status: t.status,
        flightDate: t.flightDate,
        createdAt: t.createdAt,
        profit: Number((profitValue || 0).toFixed(2))
      };
    });

    res.json({
      success: true,
      filtersApplied: {
        dateFrom: dateFrom || null,
        dateTo: dateTo || null,
        airline: airline || null,
        agentId: agentId || null,
        status: status || null,
        flightType: flightType || null,
        tripType: tripType || null
      },
      totals: {
        tickets: baseStats.totalTickets,
        segments: baseStats.totalSegments,
        passengers: {
          adults: baseStats.adults,
          children: baseStats.children,
          infants: baseStats.infants
        },
        averageProfitPerTicket: baseStats.totalTickets > 0
          ? Number((baseStats.profit / baseStats.totalTickets).toFixed(2))
          : 0
      },
      financials: {
        revenue: Number((baseStats.customerDeal || 0).toFixed(2)),
        customerPaid: Number((baseStats.customerPaid || 0).toFixed(2)),
        customerDue: Number((baseStats.customerDue || 0).toFixed(2)),
        vendorAmount: Number((baseStats.vendorAmount || 0).toFixed(2)),
        vendorPaid: Number((baseStats.vendorPaid || 0).toFixed(2)),
        vendorDue: Number((baseStats.vendorDue || 0).toFixed(2)),
        profit: Number((baseStats.profit || 0).toFixed(2)),
        netMarginPct: baseStats.customerDeal > 0
          ? Number(((baseStats.profit / baseStats.customerDeal) * 100).toFixed(2))
          : 0
      },
      statusBreakdown,
      flightTypeBreakdown: flightTypeBreakdown.map(item => ({
        flightType: item._id || 'unknown',
        count: item.count,
        revenue: Number((item.revenue || 0).toFixed(2)),
        profit: Number((item.profit || 0).toFixed(2))
      })),
      tripTypeBreakdown: tripTypeBreakdown.map(item => ({
        tripType: item._id || 'unknown',
        count: item.count,
        revenue: Number((item.revenue || 0).toFixed(2)),
        profit: Number((item.profit || 0).toFixed(2))
      })),
      topAirlines: topAirlines.map(item => ({
        airline: item._id || 'unknown',
        count: item.count,
        revenue: Number((item.revenue || 0).toFixed(2)),
        profit: Number((item.profit || 0).toFixed(2))
      })),
      topAgents: topAgents.map(item => ({
        agentId: item._id?.agentId || '',
        agent: item._id?.agent || '',
        count: item.count,
        revenue: Number((item.revenue || 0).toFixed(2)),
        profit: Number((item.profit || 0).toFixed(2))
      })),
      recentTickets: normalizedRecent
    });
  } catch (error) {
    console.error('Air ticketing dashboard summary error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch air ticketing dashboard summary',
      error: error.message
    });
  }
});

// ✅ GET: Get single Air Ticket by ID
app.get("/api/air-ticketing/tickets/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { ticketId: id },
        { bookingId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const ticket = await tickets.findOne(query);

    if (!ticket) {
      return res.status(404).json({
        success: false,
        message: 'Ticket not found'
      });
    }

    res.json({
      success: true,
      ticket
    });

  } catch (error) {
    console.error('Get ticket error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch ticket',
      error: error.message
    });
  }
});

// ✅ PUT: Update Air Ticket by ID
app.put("/api/air-ticketing/tickets/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const updateData = req.body;

    const query = {
      $or: [
        { ticketId: id },
        { bookingId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const existingTicket = await tickets.findOne(query);

    if (!existingTicket) {
      return res.status(404).json({
        success: false,
        message: 'Ticket not found'
      });
    }

    // Prepare update document
    const updateDoc = {
      updatedAt: new Date()
    };

    // Update fields if provided
    if (updateData.customerId !== undefined) updateDoc.customerId = updateData.customerId;
    if (updateData.customerName !== undefined) updateDoc.customerName = updateData.customerName;
    if (updateData.customerPhone !== undefined) updateDoc.customerPhone = updateData.customerPhone;
    if (updateData.tripType !== undefined) updateDoc.tripType = updateData.tripType;
    if (updateData.flightType !== undefined) updateDoc.flightType = updateData.flightType;
    if (updateData.date !== undefined) updateDoc.date = new Date(updateData.date);
    if (updateData.bookingId !== undefined) updateDoc.bookingId = updateData.bookingId;
    if (updateData.gdsPnr !== undefined) updateDoc.gdsPnr = updateData.gdsPnr;
    if (updateData.airlinePnr !== undefined) updateDoc.airlinePnr = updateData.airlinePnr;
    if (updateData.airline !== undefined) updateDoc.airline = updateData.airline;
    if (updateData.status !== undefined) updateDoc.status = updateData.status;
    if (updateData.origin !== undefined) updateDoc.origin = updateData.origin;
    if (updateData.destination !== undefined) updateDoc.destination = updateData.destination;
    if (updateData.flightDate !== undefined) updateDoc.flightDate = updateData.flightDate ? new Date(updateData.flightDate) : null;
    if (updateData.returnDate !== undefined) updateDoc.returnDate = updateData.returnDate ? new Date(updateData.returnDate) : null;
    if (updateData.segments !== undefined) updateDoc.segments = updateData.segments;
    if (updateData.agent !== undefined) updateDoc.agent = updateData.agent;
    if (updateData.agentId !== undefined) updateDoc.agentId = updateData.agentId;
    if (updateData.purposeType !== undefined) updateDoc.purposeType = updateData.purposeType;
    if (updateData.adultCount !== undefined) updateDoc.adultCount = parseInt(updateData.adultCount) || 0;
    if (updateData.childCount !== undefined) updateDoc.childCount = parseInt(updateData.childCount) || 0;
    if (updateData.infantCount !== undefined) updateDoc.infantCount = parseInt(updateData.infantCount) || 0;
    if (updateData.customerDeal !== undefined) updateDoc.customerDeal = parseFloat(updateData.customerDeal) || 0;
    if (updateData.customerPaid !== undefined) updateDoc.customerPaid = parseFloat(updateData.customerPaid) || 0;
    if (updateData.customerDue !== undefined) updateDoc.customerDue = parseFloat(updateData.customerDue) || 0;
    if (updateData.dueDate !== undefined) updateDoc.dueDate = updateData.dueDate ? new Date(updateData.dueDate) : null;
    if (updateData.baseFare !== undefined) updateDoc.baseFare = parseFloat(updateData.baseFare) || 0;
    if (updateData.taxBD !== undefined) updateDoc.taxBD = parseFloat(updateData.taxBD) || 0;
    if (updateData.e5 !== undefined) updateDoc.e5 = parseFloat(updateData.e5) || 0;
    if (updateData.e7 !== undefined) updateDoc.e7 = parseFloat(updateData.e7) || 0;
    if (updateData.g8 !== undefined) updateDoc.g8 = parseFloat(updateData.g8) || 0;
    if (updateData.ow !== undefined) updateDoc.ow = parseFloat(updateData.ow) || 0;
    if (updateData.p7 !== undefined) updateDoc.p7 = parseFloat(updateData.p7) || 0;
    if (updateData.p8 !== undefined) updateDoc.p8 = parseFloat(updateData.p8) || 0;
    if (updateData.ts !== undefined) updateDoc.ts = parseFloat(updateData.ts) || 0;
    if (updateData.ut !== undefined) updateDoc.ut = parseFloat(updateData.ut) || 0;
    if (updateData.yq !== undefined) updateDoc.yq = parseFloat(updateData.yq) || 0;
    if (updateData.taxes !== undefined) updateDoc.taxes = parseFloat(updateData.taxes) || 0;
    if (updateData.totalTaxes !== undefined) updateDoc.totalTaxes = parseFloat(updateData.totalTaxes) || 0;
    if (updateData.ait !== undefined) updateDoc.ait = parseFloat(updateData.ait) || 0;
    if (updateData.commissionRate !== undefined) updateDoc.commissionRate = parseFloat(updateData.commissionRate) || 0;
    if (updateData.plb !== undefined) updateDoc.plb = parseFloat(updateData.plb) || 0;
    if (updateData.salmaAirServiceCharge !== undefined) updateDoc.salmaAirServiceCharge = parseFloat(updateData.salmaAirServiceCharge) || 0;
    if (updateData.vendorServiceCharge !== undefined) updateDoc.vendorServiceCharge = parseFloat(updateData.vendorServiceCharge) || 0;
    if (updateData.vendorAmount !== undefined) updateDoc.vendorAmount = parseFloat(updateData.vendorAmount) || 0;
    if (updateData.vendorPaidFh !== undefined) updateDoc.vendorPaidFh = parseFloat(updateData.vendorPaidFh) || 0;
    if (updateData.vendorDue !== undefined) updateDoc.vendorDue = parseFloat(updateData.vendorDue) || 0;
    if (updateData.profit !== undefined) updateDoc.profit = parseFloat(updateData.profit) || 0;
    if (updateData.segmentCount !== undefined) updateDoc.segmentCount = parseInt(updateData.segmentCount) || 1;
    if (updateData.flownSegment !== undefined) updateDoc.flownSegment = updateData.flownSegment;

    // Check if financial fields are being updated
    const financialFieldsUpdated = 
      updateData.customerDeal !== undefined || 
      updateData.customerPaid !== undefined || 
      updateData.customerDue !== undefined;

    // Start transaction if financial fields are updated
    let session = null;
    try {
      if (financialFieldsUpdated && existingTicket.customerId) {
        session = client.startSession();
        await session.withTransaction(async () => {
          // Update ticket
          await tickets.updateOne(
            { _id: existingTicket._id },
            { $set: updateDoc },
            { session }
          );

          // Find customer in airCustomers collection
          const customer = await airCustomers.findOne({
            $or: [
              { customerId: existingTicket.customerId },
              { _id: ObjectId.isValid(existingTicket.customerId) ? new ObjectId(existingTicket.customerId) : null }
            ],
            isActive: { $ne: false }
          }, { session });

          if (customer) {
            // Calculate differences
            const oldDeal = parseFloat(existingTicket.customerDeal) || 0;
            const oldPaid = parseFloat(existingTicket.customerPaid) || 0;
            const oldDue = parseFloat(existingTicket.customerDue) || 0;

            const newDeal = updateData.customerDeal !== undefined ? (parseFloat(updateData.customerDeal) || 0) : oldDeal;
            const newPaid = updateData.customerPaid !== undefined ? (parseFloat(updateData.customerPaid) || 0) : oldPaid;
            const newDue = updateData.customerDue !== undefined ? (parseFloat(updateData.customerDue) || 0) : oldDue;

            const dealDiff = newDeal - oldDeal;
            const paidDiff = newPaid - oldPaid;
            const dueDiff = newDue - oldDue;

            // Update customer totals
            const currentTotalAmount = (customer.totalAmount || 0) + dealDiff;
            const currentPaidAmount = (customer.paidAmount || 0) + paidDiff;
            const currentTotalDue = (customer.totalDue || 0) + dueDiff;

            await airCustomers.updateOne(
              { _id: customer._id },
              {
                $set: {
                  totalAmount: Math.max(0, currentTotalAmount),
                  paidAmount: Math.max(0, currentPaidAmount),
                  totalDue: Math.max(0, currentTotalDue),
                  updatedAt: new Date()
                }
              },
              { session }
            );
          }
        });
      } else {
        // No financial update, just update ticket
        await tickets.updateOne(
          { _id: existingTicket._id },
          { $set: updateDoc }
        );
      }

      const updatedTicket = await tickets.findOne({ _id: existingTicket._id });

      res.json({
        success: true,
        message: 'Ticket updated successfully',
        ticket: updatedTicket
      });
    } finally {
      if (session) {
        await session.endSession();
      }
    }

  } catch (error) {
    console.error('Update ticket error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update ticket',
      error: error.message
    });
  }
});

// ✅ DELETE: Delete Air Ticket by ID (soft delete)
app.delete("/api/air-ticketing/tickets/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const query = {
      $or: [
        { ticketId: id },
        { bookingId: id },
        { _id: ObjectId.isValid(id) ? new ObjectId(id) : null }
      ],
      isActive: { $ne: false }
    };

    const ticket = await tickets.findOne(query);

    if (!ticket) {
      return res.status(404).json({
        success: false,
        message: 'Ticket not found'
      });
    }

    await tickets.updateOne(
      { _id: ticket._id },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    res.json({
      success: true,
      message: 'Ticket deleted successfully'
    });

  } catch (error) {
    console.error('Delete ticket error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to delete ticket',
      error: error.message
    });
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
    const primaryHolderObjectId = toObjectId(data.primaryHolderId);
    const seenRelationIds = new Set();
    const sanitizedRelations = Array.isArray(data.relations)
      ? data.relations
          .map((rel) => {
            const relId = toObjectId(rel?.relatedHajiId || rel?._id || rel?.id);
            if (!relId || seenRelationIds.has(String(relId))) return null;
            seenRelationIds.add(String(relId));
            return {
              relatedHajiId: relId,
              relationType: rel?.relationType || 'relative',
            };
          })
          .filter(Boolean)
      : [];
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
      area: data.area || null,
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
      manualSerialNumber: data.manualSerialNumber || '',
      pidNo: data.pidNo || '',
      ngSerialNo: data.ngSerialNo || '',
      trackingNo: data.trackingNo || '',

      photo: data.photo || data.photoUrl || '',
      passportCopy: data.passportCopy || data.passportCopyUrl || '',
      nidCopy: data.nidCopy || data.nidCopyUrl || '',

      licenseId: data.licenseId || data.license?._id || data.license?.id || '',

      primaryHolderId: primaryHolderObjectId,
      relations: sanitizedRelations,

      serviceType: 'hajj',
      serviceStatus: data.serviceStatus || (data.paymentStatus === 'paid' ? 'confirmed' : 'pending') || '',

      totalAmount: Number(data.totalAmount || 0),
      paidAmount: Number(data.paidAmount || 0),
      familyTotal: Number(data.familyTotal || 0),
      familyPaid: Number(data.familyPaid || 0),
      familyDue: Number(data.familyDue || 0),
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
    const recomputeTarget = primaryHolderObjectId || result.insertedId;
    await recomputeFamilyTotals(recomputeTarget);

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
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 20000);

    const filter = {};
    
    if (q && String(q).trim()) {
      const text = String(q).trim();
      filter.$or = [
        { name: { $regex: text, $options: 'i' } },
        { mobile: { $regex: text, $options: 'i' } },
        { email: { $regex: text, $options: 'i' } },
        { customerId: { $regex: text, $options: 'i' } },
        { passportNumber: { $regex: text, $options: 'i' } },
        { pidNo: { $regex: text, $options: 'i' } },
        { ngSerialNo: { $regex: text, $options: 'i' } },
        { trackingNo: { $regex: text, $options: 'i' } }
      ];
    }
    if (serviceStatus) filter.serviceStatus = serviceStatus;
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (isActive !== undefined) filter.isActive = String(isActive) === 'true';

    const total = await haji.countDocuments(filter);
    const rawData = await haji
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Fetch all primary holders to get their names (for dependents)
    const primaryHolderIds = rawData
      .map((doc) => doc?.primaryHolderId)
      .filter((id) => id && ObjectId.isValid(id))
      .map((id) => new ObjectId(id));
    
    const primaryHolderMap = {};
    if (primaryHolderIds.length > 0) {
      const uniquePrimaryIds = [...new Set(primaryHolderIds.map(String))].map((id) => new ObjectId(id));
      const primaryHolders = await haji
        .find({ _id: { $in: uniquePrimaryIds } })
        .toArray();
      primaryHolders.forEach((holder) => {
        primaryHolderMap[String(holder._id)] = holder.name || null;
      });
    }

    // Ensure photo/passportCopy/nidCopy fields are always present and calculate balance/due
    const data = rawData.map((doc) => {
      const totalAmount = Number(doc?.totalAmount || 0);
      const paidAmount = Number(doc?.paidAmount || 0);
      const due = Math.max(totalAmount - paidAmount, 0);
      const isDependent = doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc?._id);
      const familyTotal = Number(doc?.familyTotal || 0);
      const familyPaid = Number(doc?.familyPaid || 0);
      const familyDue = Number.isFinite(doc?.familyDue) ? Number(doc.familyDue) : Math.max(familyTotal - familyPaid, 0);
      
      // For primary holders, expose family aggregates; for dependents, hide amounts (0)
      const visibleTotal = isDependent ? 0 : (familyTotal || totalAmount);
      const visiblePaid = isDependent ? 0 : (familyPaid || paidAmount);
      const visibleDue = isDependent ? 0 : (familyDue || due);
      
      // Get primary holder name if this is a dependent
      const primaryHolderName = isDependent && doc?.primaryHolderId 
        ? primaryHolderMap[String(doc.primaryHolderId)] || null
        : null;
      
      return {
        ...doc,
        photo: doc?.photo || doc?.photoUrl || '',
        passportCopy: doc?.passportCopy || doc?.passportCopyUrl || '',
        nidCopy: doc?.nidCopy || doc?.nidCopyUrl || '',
        totalAmount: visibleTotal,
        paidAmount: visiblePaid,
        totalPaid: visiblePaid,
        due: visibleDue,
        balance: visibleDue, // alias for balance
        displayTotalAmount: visibleTotal,
        displayPaidAmount: visiblePaid,
        displayDue: visibleDue,
        familyTotal,
        familyPaid,
        familyDue,
        ...(primaryHolderName ? { primaryHolderName } : {})
      };
    });

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
    const isDependent = doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc?._id);
    const familyTotal = Number(doc?.familyTotal || 0);
    const familyPaid = Number(doc?.familyPaid || 0);
    const familyDue = Number.isFinite(doc?.familyDue) ? Number(doc.familyDue) : Math.max(familyTotal - familyPaid, 0);
    // For primary holders, expose family aggregates; for dependents, hide amounts (0)
    const visibleTotal = isDependent ? 0 : (familyTotal || totalAmount);
    const visiblePaid = isDependent ? 0 : (familyPaid || paidAmount);
    const visibleDue = isDependent ? 0 : (familyDue || due);
    const normalizedRelations = Array.isArray(doc?.relations)
      ? doc.relations.map((rel) => ({
          relatedHajiId: rel?.relatedHajiId ? String(rel.relatedHajiId) : null,
          relationType: rel?.relationType || null,
        }))
      : [];
    const hajjDue = typeof doc?.hajjDue === 'number' ? Math.max(doc.hajjDue, 0) : undefined;
    const umrahDue = typeof doc?.umrahDue === 'number' ? Math.max(doc.umrahDue, 0) : undefined;
    const normalizedPaymentStatus = (function () {
      if (paidAmount >= totalAmount && totalAmount > 0) return 'paid';
      if (paidAmount > 0 && paidAmount < totalAmount) return 'partial';
      return 'pending';
    })();
    const normalizedServiceStatus = doc?.serviceStatus || (normalizedPaymentStatus === 'paid' ? 'confirmed' : 'pending');

    // Fetch primary holder name if this is a dependent
    let primaryHolderName = null;
    if (doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc._id)) {
      const primaryHolderDoc = await haji.findOne({ _id: toObjectId(doc.primaryHolderId) });
      if (primaryHolderDoc) {
        primaryHolderName = primaryHolderDoc.name || null;
      }
    }

    res.json({
      success: true,
      data: {
        ...doc,
        area: doc?.area || null,
        photo: doc?.photo || doc?.photoUrl || '',
        passportCopy: doc?.passportCopy || doc?.passportCopyUrl || '',
        nidCopy: doc?.nidCopy || doc?.nidCopyUrl || '',
        totalAmount: visibleTotal,
        paidAmount: visiblePaid,
        totalPaid: visiblePaid,
        due: visibleDue,
        displayTotalAmount: visibleTotal,
        displayPaidAmount: visiblePaid,
        displayDue: visibleDue,
        familyTotal,
        familyPaid,
        familyDue,
        relations: normalizedRelations,
        paymentStatus: normalizedPaymentStatus,
        serviceStatus: normalizedServiceStatus || '',
        ...(typeof hajjDue === 'number' ? { hajjDue } : {}),
        ...(typeof umrahDue === 'number' ? { umrahDue } : {}),
        ...(primaryHolderName ? { primaryHolderName } : {})
      }
    });
  } catch (error) {
    console.error('Get haji error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching haji" });
  }
});

// Add/Update relation for a Haji and set primaryHolderId on related profile
app.post("/haj-umrah/haji/:id/relations", async (req, res) => {
  try {
    const { id } = req.params;
    const { relatedHajiId, relationType } = req.body || {};

    const primaryObjectId = toObjectId(id);
    const relatedObjectId = toObjectId(relatedHajiId);

    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid primary Haji ID" });
    }
    if (!relatedObjectId) {
      return res.status(400).json({ error: true, message: "Invalid related Haji ID" });
    }
    if (String(primaryObjectId) === String(relatedObjectId)) {
      return res.status(400).json({ error: true, message: "Cannot relate a Haji to themselves" });
    }

    const primaryDoc = await haji.findOne({ _id: primaryObjectId });
    if (!primaryDoc) {
      return res.status(404).json({ error: true, message: "Primary Haji not found" });
    }

    const relatedDoc = await haji.findOne({ _id: relatedObjectId });
    if (!relatedDoc) {
      return res.status(404).json({ error: true, message: "Related Haji not found" });
    }

    const updatedRelations = Array.isArray(primaryDoc.relations) ? [...primaryDoc.relations] : [];
    const existingIndex = updatedRelations.findIndex(
      (rel) => String(rel?.relatedHajiId) === String(relatedObjectId)
    );
    const newEntry = {
      relatedHajiId: relatedObjectId,
      relationType: relationType || 'relative',
    };

    if (existingIndex >= 0) {
      updatedRelations[existingIndex] = { ...updatedRelations[existingIndex], ...newEntry };
    } else {
      updatedRelations.push(newEntry);
    }

    await haji.updateOne(
      { _id: relatedObjectId },
      { $set: { primaryHolderId: primaryObjectId, updatedAt: new Date() } }
    );

    await haji.updateOne(
      { _id: primaryObjectId },
      { $set: { relations: updatedRelations, updatedAt: new Date() } }
    );

    const summary = await recomputeFamilyTotals(primaryObjectId);

    res.json({
      success: true,
      data: {
        primaryId: String(primaryObjectId),
        relations: updatedRelations.map((rel) => ({
          relatedHajiId: rel?.relatedHajiId ? String(rel.relatedHajiId) : null,
          relationType: rel?.relationType || null,
        })),
        familySummary: summary
          ? {
              familyTotal: summary.familyTotal,
              familyPaid: summary.familyPaid,
              familyDue: summary.familyDue,
            }
          : null,
      },
    });
  } catch (error) {
    console.error('Add haji relation error:', error);
    res.status(500).json({ error: true, message: "Failed to add haji relation" });
  }
});

// Delete relation for a Haji and clear primaryHolderId on related profile
app.delete("/haj-umrah/haji/:id/relations/:relatedId", async (req, res) => {
  try {
    const { id, relatedId } = req.params;

    const primaryObjectId = toObjectId(id);
    const relatedObjectId = toObjectId(relatedId);

    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid primary Haji ID" });
    }
    if (!relatedObjectId) {
      return res.status(400).json({ error: true, message: "Invalid related Haji ID" });
    }
    if (String(primaryObjectId) === String(relatedObjectId)) {
      return res.status(400).json({ error: true, message: "Cannot delete relation to itself" });
    }

    const primaryDoc = await haji.findOne({ _id: primaryObjectId });
    if (!primaryDoc) {
      return res.status(404).json({ error: true, message: "Primary Haji not found" });
    }

    const relatedDoc = await haji.findOne({ _id: relatedObjectId });
    if (!relatedDoc) {
      return res.status(404).json({ error: true, message: "Related Haji not found" });
    }

    const updatedRelations = Array.isArray(primaryDoc.relations) ? [...primaryDoc.relations] : [];
    const existingIndex = updatedRelations.findIndex(
      (rel) => String(rel?.relatedHajiId) === String(relatedObjectId)
    );

    if (existingIndex < 0) {
      return res.status(404).json({ error: true, message: "Relation not found" });
    }

    // Remove the relation from the array
    updatedRelations.splice(existingIndex, 1);

    // Clear primaryHolderId from related profile if it matches the primary
    if (String(relatedDoc.primaryHolderId) === String(primaryObjectId)) {
      await haji.updateOne(
        { _id: relatedObjectId },
        { $unset: { primaryHolderId: "" }, $set: { updatedAt: new Date() } }
      );
    }

    // Update primary profile with removed relation
    await haji.updateOne(
      { _id: primaryObjectId },
      { $set: { relations: updatedRelations, updatedAt: new Date() } }
    );

    const summary = await recomputeFamilyTotals(primaryObjectId);

    res.json({
      success: true,
      data: {
        primaryId: String(primaryObjectId),
        relations: updatedRelations.map((rel) => ({
          relatedHajiId: rel?.relatedHajiId ? String(rel.relatedHajiId) : null,
          relationType: rel?.relationType || null,
        })),
        familySummary: summary
          ? {
              familyTotal: summary.familyTotal,
              familyPaid: summary.familyPaid,
              familyDue: summary.familyDue,
            }
          : null,
      },
    });
  } catch (error) {
    console.error('Delete haji relation error:', error);
    res.status(500).json({ error: true, message: "Failed to delete haji relation" });
  }
});

// Get family summary for a primary Haji
app.get("/haj-umrah/haji/:id/family-summary", async (req, res) => {
  try {
    const { id } = req.params;
    const primaryObjectId = toObjectId(id);
    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid Haji ID" });
    }

    const summary = await recomputeFamilyTotals(primaryObjectId);
    if (!summary) {
      return res.status(404).json({ error: true, message: "Haji not found" });
    }

    const primaryIdStr = String(primaryObjectId);
    const primaryMember = (summary.members || []).find((member) => String(member?._id) === primaryIdStr);
    const relationLookup = new Map(
      (primaryMember?.relations || []).map((rel) => [
        String(rel?.relatedHajiId),
        rel?.relationType || null,
      ])
    );
    const members = (summary.members || []).map((member) => {
      const totalAmount = Number(member?.totalAmount || 0);
      const paidAmount = Number(member?.paidAmount || 0);
      const due = Math.max(totalAmount - paidAmount, 0);
      const isPrimary = String(member?._id) === primaryIdStr;
      const relationType = relationLookup.get(String(member?._id)) || null;

      return {
        _id: member?._id ? String(member._id) : null,
        name: member?.name || null,
        primaryHolderId: member?.primaryHolderId ? String(member.primaryHolderId) : null,
        totalAmount,
        paidAmount,
        due,
        displayPaidAmount: isPrimary ? paidAmount : 0,
        displayDue: isPrimary ? due : 0,
        relationType,
      };
    });

    res.json({
      success: true,
      data: {
        primaryId: primaryIdStr,
        familyTotal: summary.familyTotal,
        familyPaid: summary.familyPaid,
        familyDue: summary.familyDue,
        members,
      },
    });
  } catch (error) {
    console.error('Get family summary error:', error);
    res.status(500).json({ error: true, message: "Failed to fetch family summary" });
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

    // Ensure manualSerialNumber always exists (fallback to empty string)
    updates.manualSerialNumber = updates.manualSerialNumber || '';
    if (updates.hasOwnProperty('pidNo')) updates.pidNo = updates.pidNo || '';
    if (updates.hasOwnProperty('ngSerialNo')) updates.ngSerialNo = updates.ngSerialNo || '';
    if (updates.hasOwnProperty('trackingNo')) updates.trackingNo = updates.trackingNo || '';
    updates.photo = updates.photo || updates.photoUrl || '';
    updates.passportCopy = updates.passportCopy || updates.passportCopyUrl || '';
    updates.nidCopy = updates.nidCopy || updates.nidCopyUrl || '';
    
    if (updates.hasOwnProperty('licenseId') || updates.hasOwnProperty('license')) {
      updates.licenseId = updates.licenseId || updates.license?._id || updates.license?.id || '';
    }

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

    if (updates.hasOwnProperty('primaryHolderId')) {
      updates.primaryHolderId = updates.primaryHolderId ? toObjectId(updates.primaryHolderId) : null;
    }
    if (Array.isArray(updates.relations)) {
      const seenRelationIds = new Set();
      updates.relations = updates.relations
        .map((rel) => {
          const relId = toObjectId(rel?.relatedHajiId || rel?._id || rel?.id);
          if (!relId || seenRelationIds.has(String(relId))) return null;
          seenRelationIds.add(String(relId));
          return {
            relatedHajiId: relId,
            relationType: rel?.relationType || 'relative',
          };
        })
        .filter(Boolean);
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

    await triggerFamilyRecomputeForHaji(updatedDoc);

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

// Bulk Create Haji from Excel Upload
app.post("/haj-umrah/haji/bulk", async (req, res) => {
  try {
    const { data: hajiDataArray } = req.body || {};
    
    if (!Array.isArray(hajiDataArray) || hajiDataArray.length === 0) {
      return res.status(400).json({ 
        error: true, 
        message: "Data array is required and must not be empty" 
      });
    }

    const now = new Date();
    const results = {
      success: [],
      failed: [],
      total: hajiDataArray.length,
      successCount: 0,
      failedCount: 0
    };

    // Process each Haji record
    for (let i = 0; i < hajiDataArray.length; i++) {
      const rawData = hajiDataArray[i];
      const rowNumber = i + 1;

      try {
        // Map Excel field names to backend field names
        // Primary fields from Excel: Name, Mobile no, Fathers name, Mother's Name, Upazila, Districts
        const data = {
          // Required fields
          name: rawData['Name'] || rawData['name'] || rawData.name || '',
          mobile: rawData['Mobile no'] || rawData['Mobile No'] || rawData['mobile no'] || rawData['Mobile'] || rawData.mobile || '',
          
          // Optional fields from Excel
          fatherName: rawData['Fathers name'] || rawData['Fathers Name'] || rawData['fathers name'] || rawData['Father Name'] || rawData.fatherName || null,
          motherName: rawData['Mother\'s Name'] || rawData['Mother\'s name'] || rawData['mother\'s name'] || rawData['Mother Name'] || rawData['Mothers name'] || rawData.motherName || null,
          upazila: rawData['Upazila'] || rawData['upazila'] || rawData.upazila || null,
          district: rawData['Districts'] || rawData['districts'] || rawData['District'] || rawData['district'] || rawData.district || null,
          area: rawData['Area'] || rawData['area'] || rawData.area || null,
          
          // Additional optional fields (if provided)
          division: rawData['Division'] || rawData['division'] || rawData.division || null,
          email: rawData['Email'] || rawData['email'] || rawData.email || null,
          whatsappNo: rawData['WhatsApp'] || rawData['whatsapp'] || rawData['WhatsApp No'] || rawData.whatsappNo || null,
          address: rawData['Address'] || rawData['address'] || rawData.address || null,
          postCode: rawData['Post Code'] || rawData['post code'] || rawData['PostCode'] || rawData.postCode || null,
          passportNumber: rawData['Passport Number'] || rawData['passport number'] || rawData['Passport'] || rawData.passportNumber || null,
          nidNumber: rawData['NID Number'] || rawData['nid number'] || rawData['NID'] || rawData['nid'] || rawData.nidNumber || null,
          dateOfBirth: rawData['Date of Birth'] || rawData['date of birth'] || rawData['DOB'] || rawData['dob'] || rawData.dateOfBirth || null,
          gender: rawData['Gender'] || rawData['gender'] || rawData.gender || null,
          referenceBy: rawData['Reference By'] || rawData['reference by'] || rawData['Reference'] || rawData.referenceBy || null,
          totalAmount: rawData['Total Amount'] || rawData['total amount'] || rawData.totalAmount || 0,
          paidAmount: rawData['Paid Amount'] || rawData['paid amount'] || rawData.paidAmount || 0,
          notes: rawData['Notes'] || rawData['notes'] || rawData.notes || null,
          serviceStatus: rawData['Service Status'] || rawData['service status'] || rawData.serviceStatus || '',
          pidNo: rawData['PID No'] || rawData['pid no'] || rawData['PID'] || rawData.pidNo || '',
          ngSerialNo: rawData['NG Serial No'] || rawData['ng serial no'] || rawData['NG Serial'] || rawData.ngSerialNo || '',
          trackingNo: rawData['Tracking No'] || rawData['tracking no'] || rawData['Tracking'] || rawData.trackingNo || ''
        };

        // Validate required fields
        if (!data.name || !String(data.name).trim()) {
          throw new Error(`Row ${rowNumber}: Name is required`);
        }
        if (!data.mobile || !String(data.mobile).trim()) {
          throw new Error(`Row ${rowNumber}: Mobile is required`);
        }

        // Validate email if provided
        if (data.email) {
          const emailRegex = /^\S+@\S+\.\S+$/;
          if (!emailRegex.test(String(data.email).trim())) {
            throw new Error(`Row ${rowNumber}: Invalid email address`);
          }
        }

        // Validate date fields
        const dateFields = ["dateOfBirth"];
        for (const field of dateFields) {
          if (data[field] && !isValidDate(data[field])) {
            throw new Error(`Row ${rowNumber}: Invalid date format for ${field} (YYYY-MM-DD)`);
          }
        }

        // Generate unique Haji ID
        const hajiCustomerId = await generateCustomerId(db, 'haj');
        
        // Create Haji document
        const doc = {
          customerId: hajiCustomerId,
          name: String(data.name).trim(),
          firstName: (String(data.name).trim().split(' ')[0] || ''),
          lastName: (String(data.name).trim().split(' ').slice(1).join(' ') || ''),

          mobile: String(data.mobile).trim(),
          whatsappNo: data.whatsappNo ? String(data.whatsappNo).trim() : null,
          email: data.email ? String(data.email).trim() : null,

          address: data.address ? String(data.address).trim() : null,
          division: data.division ? String(data.division).trim() : null,
          district: data.district ? String(data.district).trim() : null,
          upazila: data.upazila ? String(data.upazila).trim() : null,
          area: data.area ? String(data.area).trim() : null,
          postCode: data.postCode ? String(data.postCode).trim() : null,

          passportNumber: data.passportNumber ? String(data.passportNumber).trim() : null,
          passportType: 'ordinary',
          issueDate: null,
          expiryDate: null,
          dateOfBirth: data.dateOfBirth || null,
          nidNumber: data.nidNumber ? String(data.nidNumber).trim() : null,
          passportFirstName: (String(data.name).trim().split(' ')[0] || ''),
          passportLastName: (String(data.name).trim().split(' ').slice(1).join(' ') || ''),
          nationality: 'Bangladeshi',
          gender: data.gender ? String(data.gender).toLowerCase() : 'male',

          fatherName: data.fatherName ? String(data.fatherName).trim() : null,
          motherName: data.motherName ? String(data.motherName).trim() : null,
          spouseName: null,
          maritalStatus: 'single',

          occupation: null,
          customerImage: null,
          notes: data.notes ? String(data.notes).trim() : null,
          isActive: true,

          referenceBy: data.referenceBy ? String(data.referenceBy).trim() : null,
          manualSerialNumber: rawData.manualSerialNumber || rawData['Manual Serial Number'] || rawData['manual serial number'] || '',
          pidNo: data.pidNo || '',
          ngSerialNo: data.ngSerialNo || '',
          trackingNo: data.trackingNo || '',

          serviceType: 'hajj',
          serviceStatus: data.serviceStatus || '',

          totalAmount: Number(data.totalAmount || 0),
          paidAmount: Number(data.paidAmount || 0),
          paymentMethod: 'cash',
          paymentStatus: (function () {
            const total = Number(data.totalAmount || 0);
            const paid = Number(data.paidAmount || 0);
            if (paid >= total && total > 0) return 'paid';
            if (paid > 0 && paid < total) return 'partial';
            return 'pending';
          })(),

          packageInfo: {
            packageId: null,
            packageName: null,
            packageType: 'hajj',
            agentId: null,
            agent: null,
            agentContact: null,
            departureDate: null,
            returnDate: null,
            previousHajj: false,
            previousUmrah: false,
            specialRequirements: null
          },

          createdAt: now,
          updatedAt: now,
          deletedAt: null
        };

        // Insert the document
        const result = await haji.insertOne(doc);
        results.success.push({
          row: rowNumber,
          _id: result.insertedId,
          customerId: doc.customerId,
          name: doc.name,
          mobile: doc.mobile
        });
        results.successCount++;

      } catch (error) {
        results.failed.push({
          row: rowNumber,
          data: rawData,
          error: error.message || 'Unknown error'
        });
        results.failedCount++;
      }
    }

    // Return results
    return res.status(200).json({
      success: true,
      message: `Processed ${results.total} records. ${results.successCount} succeeded, ${results.failedCount} failed.`,
      data: results
    });

  } catch (error) {
    console.error('Bulk create haji error:', error);
    res.status(500).json({ 
      error: true, 
      message: "Internal server error while bulk creating haji",
      details: error.message 
    });
  }
});

// Get Haji transaction history
app.get("/haj-umrah/haji/:id/transactions", async (req, res) => {
  try {
    const { id } = req.params;
    const { fromDate, toDate, transactionType, page = 1, limit = 20 } = req.query || {};

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: true,
        message: "Invalid Haji ID"
      });
    }

    // Verify Haji exists
    const hajiDoc = await haji.findOne({ _id: new ObjectId(id) });
    if (!hajiDoc) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Haji not found"
      });
    }

    const hajiIdStr = String(id);
    const hajiObjectId = new ObjectId(id);
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    // Build filter - match transactions where partyType is 'haji' and partyId matches
    const filter = {
      isActive: { $ne: false },
      partyType: 'haji',
      $or: [
        { partyId: hajiIdStr },
        { partyId: hajiObjectId },
        { partyId: hajiDoc.customerId } // Also match by customerId
      ]
    };

    // Add transaction type filter if provided
    if (transactionType) {
      filter.transactionType = String(transactionType);
    }

    // Add date range filter if provided
    if (fromDate || toDate) {
      filter.date = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (!isNaN(start.getTime())) {
          filter.date.$gte = start;
        }
      }
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
          filter.date.$lte = end;
        }
      }
    }

    // Fetch transactions with pagination
    const cursor = transactions
      .find(filter)
      .sort({ date: -1, createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum);

    // Calculate totals (credit and debit separately)
    const totalsPipeline = [
      { $match: filter },
      {
        $group: {
          _id: null,
          totalCredit: {
            $sum: {
              $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0]
            }
          },
          totalDebit: {
            $sum: {
              $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0]
            }
          },
          count: { $sum: 1 }
        }
      }
    ];

    const [items, total, totalsResult] = await Promise.all([
      cursor.toArray(),
      transactions.countDocuments(filter),
      transactions.aggregate(totalsPipeline).toArray()
    ]);

    const totals = totalsResult[0] || {
      totalCredit: 0,
      totalDebit: 0,
      count: 0
    };

    const netAmount = Number(totals.totalCredit) - Number(totals.totalDebit);

    // Format transaction data
    const data = items.map((tx) => ({
      _id: String(tx._id),
      transactionId: tx.transactionId || null,
      transactionType: tx.transactionType || null,
      amount: Number(tx.amount || 0),
      date: tx.date || tx.createdAt || null,
      serviceCategory: tx.serviceCategory || null,
      subCategory: tx.subCategory || null,
      paymentMethod: tx.paymentMethod || null,
      notes: tx.notes || null,
      reference: tx.reference || null,
      invoiceId: tx.invoiceId || null,
      branchId: tx.branchId || null,
      createdBy: tx.createdBy || null,
      createdAt: tx.createdAt || null,
      updatedAt: tx.updatedAt || null,
      partyName: tx.partyName || hajiDoc.name || null,
      targetAccountId: tx.targetAccountId || null,
      targetAccountName: tx.targetAccountName || null
    }));

    res.json({
      success: true,
      data,
      summary: {
        totalTransactions: totals.count || 0,
        totalCredit: Number(totals.totalCredit || 0),
        totalDebit: Number(totals.totalDebit || 0),
        netAmount: Number(netAmount),
        balance: Number(netAmount) // alias for netAmount
      },
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get Haji transaction history error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Failed to fetch transaction history",
      details: error.message
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
    const primaryHolderObjectId = toObjectId(data.primaryHolderId);
    const seenRelationIds = new Set();
    const sanitizedRelations = Array.isArray(data.relations)
      ? data.relations
          .map((rel) => {
            const relId = toObjectId(rel?.relatedHajiId || rel?.relatedUmrahId || rel?._id || rel?.id);
            if (!relId || seenRelationIds.has(String(relId))) return null;
            seenRelationIds.add(String(relId));
            return {
              relatedUmrahId: relId,
              relationType: rel?.relationType || 'relative',
            };
          })
          .filter(Boolean)
      : [];
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
      area: data.area || null,
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
      manualSerialNumber: data.manualSerialNumber || '',
      pidNo: data.pidNo || '',
      ngSerialNo: data.ngSerialNo || '',
      trackingNo: data.trackingNo || '',

      photo: data.photo || data.photoUrl || '',
      photoUrl: data.photo || data.photoUrl || '',
      passportCopy: data.passportCopy || data.passportCopyUrl || '',
      passportCopyUrl: data.passportCopy || data.passportCopyUrl || '',
      nidCopy: data.nidCopy || data.nidCopyUrl || '',
      nidCopyUrl: data.nidCopy || data.nidCopyUrl || '',

      primaryHolderId: primaryHolderObjectId,
      relations: sanitizedRelations,

      serviceType: 'umrah',
      serviceStatus: data.serviceStatus || (data.paymentStatus === 'paid' ? 'confirmed' : 'pending'),

      totalAmount: Number(data.totalAmount || 0),
      paidAmount: Number(data.paidAmount || 0),
      familyTotal: Number(data.familyTotal || 0),
      familyPaid: Number(data.familyPaid || 0),
      familyDue: Number(data.familyDue || 0),
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
    const recomputeTarget = primaryHolderObjectId || result.insertedId;
    await recomputeUmrahFamilyTotals(recomputeTarget);

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
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 20000);

    const filter = {};
    if (q && String(q).trim()) {
      const text = String(q).trim();
      filter.$or = [
        { name: { $regex: text, $options: 'i' } },
        { mobile: { $regex: text, $options: 'i' } },
        { email: { $regex: text, $options: 'i' } },
        { customerId: { $regex: text, $options: 'i' } },
        { passportNumber: { $regex: text, $options: 'i' } },
        { pidNo: { $regex: text, $options: 'i' } },
        { ngSerialNo: { $regex: text, $options: 'i' } },
        { trackingNo: { $regex: text, $options: 'i' } }
      ];
    }
    if (serviceStatus) filter.serviceStatus = serviceStatus;
    if (paymentStatus) filter.paymentStatus = paymentStatus;
    if (isActive !== undefined) filter.isActive = String(isActive) === 'true';

    const total = await umrah.countDocuments(filter);
    const rawData = await umrah
      .find(filter)
      .sort({ createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum)
      .toArray();

    // Fetch all primary holders to get their names (for dependents)
    const primaryHolderIds = rawData
      .map((doc) => doc?.primaryHolderId)
      .filter((id) => id && ObjectId.isValid(id))
      .map((id) => new ObjectId(id));
    
    const primaryHolderMap = {};
    if (primaryHolderIds.length > 0) {
      const uniquePrimaryIds = [...new Set(primaryHolderIds.map(String))].map((id) => new ObjectId(id));
      const primaryHolders = await umrah
        .find({ _id: { $in: uniquePrimaryIds } })
        .toArray();
      primaryHolders.forEach((holder) => {
        primaryHolderMap[String(holder._id)] = holder.name || null;
      });
    }

    // Add primaryHolderName to each document if it's a dependent
    const data = rawData.map((doc) => {
      const isDependent = doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc._id);
      const primaryHolderName = isDependent && doc?.primaryHolderId 
        ? primaryHolderMap[String(doc.primaryHolderId)] || null
        : null;
      
      return {
        ...doc,
        ...(primaryHolderName ? { primaryHolderName } : {})
      };
    });

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
    const isDependent = doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc?._id);
    const familyTotal = Number(doc?.familyTotal || 0);
    const familyPaid = Number(doc?.familyPaid || 0);
    const familyDue = Number.isFinite(doc?.familyDue) ? Number(doc.familyDue) : Math.max(familyTotal - familyPaid, 0);
    const visibleTotal = isDependent ? 0 : (familyTotal || totalAmount);
    const visiblePaid = isDependent ? 0 : (familyPaid || paidAmount);
    const visibleDue = isDependent ? 0 : (familyDue || due);
    const normalizedRelations = Array.isArray(doc?.relations)
      ? doc.relations.map((rel) => ({
          relatedUmrahId: rel?.relatedUmrahId ? String(rel.relatedUmrahId) : null,
          relationType: rel?.relationType || null,
        }))
      : [];
    const hajjDue = typeof doc?.hajjDue === 'number' ? Math.max(doc.hajjDue, 0) : undefined;
    const umrahDue = typeof doc?.umrahDue === 'number' ? Math.max(doc.umrahDue, 0) : undefined;
    const normalizedPaymentStatus = (function () {
      if (paidAmount >= totalAmount && totalAmount > 0) return 'paid';
      if (paidAmount > 0 && paidAmount < totalAmount) return 'partial';
      return 'pending';
    })();
    const normalizedServiceStatus = doc?.serviceStatus || (normalizedPaymentStatus === 'paid' ? 'confirmed' : 'pending');

    // Fetch primary holder name if this is a dependent
    let primaryHolderName = null;
    if (doc?.primaryHolderId && String(doc.primaryHolderId) !== String(doc._id)) {
      const primaryHolderDoc = await umrah.findOne({ _id: toObjectId(doc.primaryHolderId) });
      if (primaryHolderDoc) {
        primaryHolderName = primaryHolderDoc.name || null;
      }
    }

    res.json({
      success: true,
      data: {
        ...doc,
        area: doc?.area || null,
        totalAmount: visibleTotal,
        paidAmount: visiblePaid,
        totalPaid: visiblePaid,
        due: visibleDue,
        displayTotalAmount: visibleTotal,
        displayPaidAmount: visiblePaid,
        displayDue: visibleDue,
        familyTotal,
        familyPaid,
        familyDue,
        relations: normalizedRelations,
        ...(typeof hajjDue === 'number' ? { hajjDue } : {}),
        ...(typeof umrahDue === 'number' ? { umrahDue } : {}),
        paymentStatus: normalizedPaymentStatus,
        serviceStatus: normalizedServiceStatus,
        ...(primaryHolderName ? { primaryHolderName } : {})
      }
    });
  } catch (error) {
    console.error('Get umrah error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching umrah" });
  }
});

// Add/Update relation for an Umrah profile and set primaryHolderId on related profile
app.post("/haj-umrah/umrah/:id/relations", async (req, res) => {
  try {
    const { id } = req.params;
    const { relatedUmrahId, relationType } = req.body || {};

    const primaryObjectId = toObjectId(id);
    const relatedObjectId = toObjectId(relatedUmrahId);

    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid primary Umrah ID" });
    }
    if (!relatedObjectId) {
      return res.status(400).json({ error: true, message: "Invalid related Umrah ID" });
    }
    if (String(primaryObjectId) === String(relatedObjectId)) {
      return res.status(400).json({ error: true, message: "Cannot relate a profile to itself" });
    }

    const primaryDoc = await umrah.findOne({ _id: primaryObjectId });
    if (!primaryDoc) {
      return res.status(404).json({ error: true, message: "Primary Umrah not found" });
    }

    const relatedDoc = await umrah.findOne({ _id: relatedObjectId });
    if (!relatedDoc) {
      return res.status(404).json({ error: true, message: "Related Umrah not found" });
    }

    const updatedRelations = Array.isArray(primaryDoc.relations) ? [...primaryDoc.relations] : [];
    const existingIndex = updatedRelations.findIndex(
      (rel) => String(rel?.relatedUmrahId) === String(relatedObjectId)
    );
    const newEntry = {
      relatedUmrahId: relatedObjectId,
      relationType: relationType || 'relative',
    };

    if (existingIndex >= 0) {
      updatedRelations[existingIndex] = { ...updatedRelations[existingIndex], ...newEntry };
    } else {
      updatedRelations.push(newEntry);
    }

    await umrah.updateOne(
      { _id: relatedObjectId },
      { $set: { primaryHolderId: primaryObjectId, updatedAt: new Date() } }
    );

    await umrah.updateOne(
      { _id: primaryObjectId },
      { $set: { relations: updatedRelations, updatedAt: new Date() } }
    );

    const summary = await recomputeUmrahFamilyTotals(primaryObjectId);

    res.json({
      success: true,
      data: {
        primaryId: String(primaryObjectId),
        relations: updatedRelations.map((rel) => ({
          relatedUmrahId: rel?.relatedUmrahId ? String(rel.relatedUmrahId) : null,
          relationType: rel?.relationType || null,
        })),
        familySummary: summary
          ? {
              familyTotal: summary.familyTotal,
              familyPaid: summary.familyPaid,
              familyDue: summary.familyDue,
            }
          : null,
      },
    });
  } catch (error) {
    console.error('Add umrah relation error:', error);
    res.status(500).json({ error: true, message: "Failed to add umrah relation" });
  }
});

// Delete relation for an Umrah profile and clear primaryHolderId on related profile
app.delete("/haj-umrah/umrah/:id/relations/:relatedId", async (req, res) => {
  try {
    const { id, relatedId } = req.params;

    const primaryObjectId = toObjectId(id);
    const relatedObjectId = toObjectId(relatedId);

    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid primary Umrah ID" });
    }
    if (!relatedObjectId) {
      return res.status(400).json({ error: true, message: "Invalid related Umrah ID" });
    }
    if (String(primaryObjectId) === String(relatedObjectId)) {
      return res.status(400).json({ error: true, message: "Cannot delete relation to itself" });
    }

    const primaryDoc = await umrah.findOne({ _id: primaryObjectId });
    if (!primaryDoc) {
      return res.status(404).json({ error: true, message: "Primary Umrah not found" });
    }

    const relatedDoc = await umrah.findOne({ _id: relatedObjectId });
    if (!relatedDoc) {
      return res.status(404).json({ error: true, message: "Related Umrah not found" });
    }

    const updatedRelations = Array.isArray(primaryDoc.relations) ? [...primaryDoc.relations] : [];
    const existingIndex = updatedRelations.findIndex(
      (rel) => String(rel?.relatedUmrahId) === String(relatedObjectId)
    );

    if (existingIndex < 0) {
      return res.status(404).json({ error: true, message: "Relation not found" });
    }

    // Remove the relation from the array
    updatedRelations.splice(existingIndex, 1);

    // Clear primaryHolderId from related profile if it matches the primary
    if (String(relatedDoc.primaryHolderId) === String(primaryObjectId)) {
      await umrah.updateOne(
        { _id: relatedObjectId },
        { $unset: { primaryHolderId: "" }, $set: { updatedAt: new Date() } }
      );
    }

    // Update primary profile with removed relation
    await umrah.updateOne(
      { _id: primaryObjectId },
      { $set: { relations: updatedRelations, updatedAt: new Date() } }
    );

    const summary = await recomputeUmrahFamilyTotals(primaryObjectId);

    res.json({
      success: true,
      data: {
        primaryId: String(primaryObjectId),
        relations: updatedRelations.map((rel) => ({
          relatedUmrahId: rel?.relatedUmrahId ? String(rel.relatedUmrahId) : null,
          relationType: rel?.relationType || null,
        })),
        familySummary: summary
          ? {
              familyTotal: summary.familyTotal,
              familyPaid: summary.familyPaid,
              familyDue: summary.familyDue,
            }
          : null,
      },
    });
  } catch (error) {
    console.error('Delete umrah relation error:', error);
    res.status(500).json({ error: true, message: "Failed to delete umrah relation" });
  }
});

// Get family summary for a primary Umrah
app.get("/haj-umrah/umrah/:id/family-summary", async (req, res) => {
  try {
    const { id } = req.params;
    const primaryObjectId = toObjectId(id);
    if (!primaryObjectId) {
      return res.status(400).json({ error: true, message: "Invalid Umrah ID" });
    }

    const summary = await recomputeUmrahFamilyTotals(primaryObjectId);
    if (!summary) {
      return res.status(404).json({ error: true, message: "Umrah not found" });
    }

    const primaryIdStr = String(primaryObjectId);
    const primaryMember = (summary.members || []).find((member) => String(member?._id) === primaryIdStr);
    const relationLookup = new Map(
      (primaryMember?.relations || []).map((rel) => [
        String(rel?.relatedUmrahId),
        rel?.relationType || null,
      ])
    );

    const members = (summary.members || []).map((member) => {
      const totalAmount = Number(member?.totalAmount || 0);
      const paidAmount = Number(member?.paidAmount || 0);
      const due = Math.max(totalAmount - paidAmount, 0);
      const isPrimary = String(member?._id) === primaryIdStr;
      const relationType = relationLookup.get(String(member?._id)) || null;

      return {
        _id: member?._id ? String(member._id) : null,
        name: member?.name || null,
        primaryHolderId: member?.primaryHolderId ? String(member.primaryHolderId) : null,
        totalAmount,
        paidAmount,
        due,
        displayPaidAmount: isPrimary ? paidAmount : 0,
        displayDue: isPrimary ? due : 0,
        relationType,
      };
    });

    res.json({
      success: true,
      data: {
        primaryId: primaryIdStr,
        familyTotal: summary.familyTotal,
        familyPaid: summary.familyPaid,
        familyDue: summary.familyDue,
        members,
      },
    });
  } catch (error) {
    console.error('Get umrah family summary error:', error);
    res.status(500).json({ error: true, message: "Failed to fetch family summary" });
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

    updates.photo = updates.photo || updates.photoUrl || '';
    updates.photoUrl = updates.photo || updates.photoUrl || '';
    updates.passportCopy = updates.passportCopy || updates.passportCopyUrl || '';
    updates.passportCopyUrl = updates.passportCopy || updates.passportCopyUrl || '';
    updates.nidCopy = updates.nidCopy || updates.nidCopyUrl || '';
    updates.nidCopyUrl = updates.nidCopy || updates.nidCopyUrl || '';
    if (updates.hasOwnProperty('pidNo')) updates.pidNo = updates.pidNo || '';
    if (updates.hasOwnProperty('ngSerialNo')) updates.ngSerialNo = updates.ngSerialNo || '';
    if (updates.hasOwnProperty('trackingNo')) updates.trackingNo = updates.trackingNo || '';

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

    if (updates.hasOwnProperty('primaryHolderId')) {
      updates.primaryHolderId = updates.primaryHolderId ? toObjectId(updates.primaryHolderId) : null;
    }
    if (Array.isArray(updates.relations)) {
      const seenRelationIds = new Set();
      updates.relations = updates.relations
        .map((rel) => {
          const relId = toObjectId(rel?.relatedUmrahId || rel?._id || rel?.id);
          if (!relId || seenRelationIds.has(String(relId))) return null;
          seenRelationIds.add(String(relId));
          return {
            relatedUmrahId: relId,
            relationType: rel?.relationType || 'relative',
          };
        })
        .filter(Boolean);
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

    await triggerFamilyRecomputeForUmrah(updatedDoc);

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

// Bulk Create Umrah from Excel Upload
app.post("/haj-umrah/umrah/bulk", async (req, res) => {
  try {
    const { data: umrahDataArray } = req.body || {};
    
    if (!Array.isArray(umrahDataArray) || umrahDataArray.length === 0) {
      return res.status(400).json({ 
        error: true, 
        message: "Data array is required and must not be empty" 
      });
    }

    const now = new Date();
    const results = {
      success: [],
      failed: [],
      total: umrahDataArray.length,
      successCount: 0,
      failedCount: 0
    };

    // Process each Umrah record
    for (let i = 0; i < umrahDataArray.length; i++) {
      const rawData = umrahDataArray[i];
      const rowNumber = i + 1;

      try {
        // Map Excel field names to backend field names
        // Primary fields from Excel: Name, Mobile no, Fathers name, Mother's Name, Upazila, Districts
        const data = {
          // Required fields
          name: rawData['Name'] || rawData['name'] || rawData.name || '',
          mobile: rawData['Mobile no'] || rawData['Mobile No'] || rawData['mobile no'] || rawData['Mobile'] || rawData.mobile || '',
          
          // Optional fields from Excel
          fatherName: rawData['Fathers name'] || rawData['Fathers Name'] || rawData['fathers name'] || rawData['Father Name'] || rawData.fatherName || null,
          motherName: rawData['Mother\'s Name'] || rawData['Mother\'s name'] || rawData['mother\'s name'] || rawData['Mother Name'] || rawData['Mothers name'] || rawData.motherName || null,
          upazila: rawData['Upazila'] || rawData['upazila'] || rawData.upazila || null,
          district: rawData['Districts'] || rawData['districts'] || rawData['District'] || rawData['district'] || rawData.district || null,
          area: rawData['Area'] || rawData['area'] || rawData.area || null,
          
          // Additional optional fields (if provided)
          division: rawData['Division'] || rawData['division'] || rawData.division || null,
          email: rawData['Email'] || rawData['email'] || rawData.email || null,
          whatsappNo: rawData['WhatsApp'] || rawData['whatsapp'] || rawData['WhatsApp No'] || rawData.whatsappNo || null,
          address: rawData['Address'] || rawData['address'] || rawData.address || null,
          postCode: rawData['Post Code'] || rawData['post code'] || rawData['PostCode'] || rawData.postCode || null,
          passportNumber: rawData['Passport Number'] || rawData['passport number'] || rawData['Passport'] || rawData.passportNumber || null,
          nidNumber: rawData['NID Number'] || rawData['nid number'] || rawData['NID'] || rawData['nid'] || rawData.nidNumber || null,
          dateOfBirth: rawData['Date of Birth'] || rawData['date of birth'] || rawData['DOB'] || rawData['dob'] || rawData.dateOfBirth || null,
          gender: rawData['Gender'] || rawData['gender'] || rawData.gender || null,
          referenceBy: rawData['Reference By'] || rawData['reference by'] || rawData['Reference'] || rawData.referenceBy || null,
          totalAmount: rawData['Total Amount'] || rawData['total amount'] || rawData.totalAmount || 0,
          paidAmount: rawData['Paid Amount'] || rawData['paid amount'] || rawData.paidAmount || 0,
          notes: rawData['Notes'] || rawData['notes'] || rawData.notes || null,
          serviceStatus: rawData['Service Status'] || rawData['service status'] || rawData.serviceStatus || '',
          pidNo: rawData['PID No'] || rawData['pid no'] || rawData['PID'] || rawData.pidNo || '',
          ngSerialNo: rawData['NG Serial No'] || rawData['ng serial no'] || rawData['NG Serial'] || rawData.ngSerialNo || '',
          trackingNo: rawData['Tracking No'] || rawData['tracking no'] || rawData['Tracking'] || rawData.trackingNo || ''
        };

        // Validate required fields
        if (!data.name || !String(data.name).trim()) {
          throw new Error(`Row ${rowNumber}: Name is required`);
        }
        if (!data.mobile || !String(data.mobile).trim()) {
          throw new Error(`Row ${rowNumber}: Mobile is required`);
        }

        // Validate email if provided
        if (data.email) {
          const emailRegex = /^\S+@\S+\.\S+$/;
          if (!emailRegex.test(String(data.email).trim())) {
            throw new Error(`Row ${rowNumber}: Invalid email address`);
          }
        }

        // Validate date fields
        const dateFields = ["dateOfBirth"];
        for (const field of dateFields) {
          if (data[field] && !isValidDate(data[field])) {
            throw new Error(`Row ${rowNumber}: Invalid date format for ${field} (YYYY-MM-DD)`);
          }
        }

        // Generate unique Umrah ID
        const umrahCustomerId = await generateCustomerId(db, 'umrah');
        
        // Create Umrah document
        const doc = {
          customerId: umrahCustomerId,
          name: String(data.name).trim(),
          firstName: (String(data.name).trim().split(' ')[0] || ''),
          lastName: (String(data.name).trim().split(' ').slice(1).join(' ') || ''),

          mobile: String(data.mobile).trim(),
          whatsappNo: data.whatsappNo ? String(data.whatsappNo).trim() : null,
          email: data.email ? String(data.email).trim() : null,

          address: data.address ? String(data.address).trim() : null,
          division: data.division ? String(data.division).trim() : null,
          district: data.district ? String(data.district).trim() : null,
          upazila: data.upazila ? String(data.upazila).trim() : null,
          area: data.area ? String(data.area).trim() : null,
          postCode: data.postCode ? String(data.postCode).trim() : null,

          passportNumber: data.passportNumber ? String(data.passportNumber).trim() : null,
          passportType: 'ordinary',
          issueDate: null,
          expiryDate: null,
          dateOfBirth: data.dateOfBirth || null,
          nidNumber: data.nidNumber ? String(data.nidNumber).trim() : null,
          passportFirstName: (String(data.name).trim().split(' ')[0] || ''),
          passportLastName: (String(data.name).trim().split(' ').slice(1).join(' ') || ''),
          nationality: 'Bangladeshi',
          gender: data.gender ? String(data.gender).toLowerCase() : 'male',

          fatherName: data.fatherName ? String(data.fatherName).trim() : null,
          motherName: data.motherName ? String(data.motherName).trim() : null,
          spouseName: null,
          maritalStatus: 'single',

          occupation: null,
          customerImage: null,
          notes: data.notes ? String(data.notes).trim() : null,
          isActive: true,

          referenceBy: data.referenceBy ? String(data.referenceBy).trim() : null,
        manualSerialNumber: rawData.manualSerialNumber || rawData['Manual Serial Number'] || rawData['manual serial number'] || '',
          pidNo: data.pidNo || '',
          ngSerialNo: data.ngSerialNo || '',
          trackingNo: data.trackingNo || '',

          serviceType: 'umrah',
          serviceStatus: data.serviceStatus || '',

          totalAmount: Number(data.totalAmount || 0),
          paidAmount: Number(data.paidAmount || 0),
          paymentMethod: 'cash',
          paymentStatus: (function () {
            const total = Number(data.totalAmount || 0);
            const paid = Number(data.paidAmount || 0);
            if (paid >= total && total > 0) return 'paid';
            if (paid > 0 && paid < total) return 'partial';
            return 'pending';
          })(),

          packageInfo: {
            packageId: null,
            packageName: null,
            packageType: 'umrah',
            agentId: null,
            agent: null,
            agentContact: null,
            departureDate: null,
            returnDate: null,
            previousHajj: false,
            previousUmrah: false,
            specialRequirements: null
          },

          createdAt: now,
          updatedAt: now,
          deletedAt: null
        };

        // Insert the document
        const result = await umrah.insertOne(doc);
        results.success.push({
          row: rowNumber,
          _id: result.insertedId,
          customerId: doc.customerId,
          name: doc.name,
          mobile: doc.mobile
        });
        results.successCount++;

      } catch (error) {
        results.failed.push({
          row: rowNumber,
          data: rawData,
          error: error.message || 'Unknown error'
        });
        results.failedCount++;
      }
    }

    // Return results
    return res.status(200).json({
      success: true,
      message: `Processed ${results.total} records. ${results.successCount} succeeded, ${results.failedCount} failed.`,
      data: results
    });

  } catch (error) {
    console.error('Bulk create umrah error:', error);
    res.status(500).json({ 
      error: true, 
      message: "Internal server error while bulk creating umrah",
      details: error.message 
    });
  }
});

// Get Umrah transaction history
app.get("/haj-umrah/umrah/:id/transactions", async (req, res) => {
  try {
    const { id } = req.params;
    const { fromDate, toDate, transactionType, page = 1, limit = 20 } = req.query || {};

    // Check if id is valid ObjectId or customerId
    const isOid = ObjectId.isValid(id);
    const cond = isOid
      ? { $or: [{ _id: new ObjectId(id) }, { customerId: id }] }
      : { customerId: id };

    // Verify Umrah exists
    const umrahDoc = await umrah.findOne(cond);
    if (!umrahDoc) {
      return res.status(404).json({
        success: false,
        error: true,
        message: "Umrah not found"
      });
    }

    const umrahIdStr = String(umrahDoc._id);
    const umrahObjectId = umrahDoc._id;
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    // Build filter - match transactions where partyType is 'umrah' and partyId matches
    const filter = {
      isActive: { $ne: false },
      partyType: 'umrah',
      $or: [
        { partyId: umrahIdStr },
        { partyId: umrahObjectId },
        { partyId: umrahDoc.customerId } // Also match by customerId
      ]
    };

    // Add transaction type filter if provided
    if (transactionType) {
      filter.transactionType = String(transactionType);
    }

    // Add date range filter if provided
    if (fromDate || toDate) {
      filter.date = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (!isNaN(start.getTime())) {
          filter.date.$gte = start;
        }
      }
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
          filter.date.$lte = end;
        }
      }
    }

    // Fetch transactions with pagination
    const cursor = transactions
      .find(filter)
      .sort({ date: -1, createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum);

    // Calculate totals (credit and debit separately)
    const totalsPipeline = [
      { $match: filter },
      {
        $group: {
          _id: null,
          totalCredit: {
            $sum: {
              $cond: [{ $eq: ["$transactionType", "credit"] }, "$amount", 0]
            }
          },
          totalDebit: {
            $sum: {
              $cond: [{ $eq: ["$transactionType", "debit"] }, "$amount", 0]
            }
          },
          count: { $sum: 1 }
        }
      }
    ];

    const [items, total, totalsResult] = await Promise.all([
      cursor.toArray(),
      transactions.countDocuments(filter),
      transactions.aggregate(totalsPipeline).toArray()
    ]);

    const totals = totalsResult[0] || {
      totalCredit: 0,
      totalDebit: 0,
      count: 0
    };

    const netAmount = Number(totals.totalCredit) - Number(totals.totalDebit);

    // Format transaction data
    const data = items.map((tx) => ({
      _id: String(tx._id),
      transactionId: tx.transactionId || null,
      transactionType: tx.transactionType || null,
      amount: Number(tx.amount || 0),
      date: tx.date || tx.createdAt || null,
      serviceCategory: tx.serviceCategory || null,
      subCategory: tx.subCategory || null,
      paymentMethod: tx.paymentMethod || null,
      notes: tx.notes || null,
      reference: tx.reference || null,
      invoiceId: tx.invoiceId || null,
      branchId: tx.branchId || null,
      createdBy: tx.createdBy || null,
      createdAt: tx.createdAt || null,
      updatedAt: tx.updatedAt || null,
      partyName: tx.partyName || umrahDoc.name || null,
      targetAccountId: tx.targetAccountId || null,
      targetAccountName: tx.targetAccountName || null
    }));

    res.json({
      success: true,
      data,
      summary: {
        totalTransactions: totals.count || 0,
        totalCredit: Number(totals.totalCredit || 0),
        totalDebit: Number(totals.totalDebit || 0),
        netAmount: Number(netAmount),
        balance: Number(netAmount) // alias for netAmount
      },
      pagination: {
        page: pageNum,
        limit: limitNum,
        total: total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get Umrah transaction history error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: "Failed to fetch transaction history",
      details: error.message
    });
  }
});

// Helper: Generate unique Haj-Umrah Agent ID
const generateHajUmrahAgentId = async (db) => {
  const counterCollection = db.collection("counters");
  
  // Create counter key for haj-umrah agent
  const counterKey = `haj_umrah_agent`;
  
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
  
  // Format: HU + 00001 (e.g., HU00001)
  const serial = String(newSequence).padStart(5, '0');
  
  return `HUAGE${serial}`;
};

// ==================== AGENT ROUTES ====================
// Helper functions for financial calculations
// Helper function to safely convert mixed string/number values
const toNumeric = (value) => {
  if (value === undefined || value === null) return null;
  if (typeof value === 'number' && !Number.isNaN(value)) return value;
  if (typeof value === 'string') {
    const cleaned = value.replace(/[^0-9.-]/g, '');
    if (!cleaned) return null;
    const numericValue = Number(cleaned);
    return Number.isNaN(numericValue) ? null : numericValue;
  }
  return null;
};

// Helper function to resolve number from multiple possible values
const resolveNumber = (...values) => {
  for (const value of values) {
    const numericValue = toNumeric(value);
    if (numericValue !== null) {
      return numericValue;
    }
  }
  return 0;
};

// Calculate profit/loss for a package
const calculateProfitLoss = (pkg = {}) => {
  const totals = pkg.totals || {};
  const profitLossFromApi = pkg.profitLoss || {};

  const costingPrice =
    resolveNumber(
      profitLossFromApi.totalCostingPrice,
      profitLossFromApi.costingPrice,
      totals.costingPrice,
      totals.grandTotal,
      pkg.costingPrice
    ) || 0;

  const packagePrice =
    resolveNumber(
      profitLossFromApi.packagePrice,
      pkg.totalPrice,
      totals.packagePrice,
      totals.subtotal,
      totals.grandTotal
    ) || 0;

  const profitValue =
    resolveNumber(
      profitLossFromApi.profitOrLoss,
      profitLossFromApi.profitLoss
    ) || (packagePrice - costingPrice);

  return {
    costingPrice,
    packagePrice,
    profitValue,
  };
};

// Check if package is Hajj type
const isHajjPackage = (pkg) => {
  return (
    pkg.packageType === 'Hajj' ||
    pkg.packageType === 'হজ্জ' ||
    pkg.customPackageType === 'Custom Hajj' ||
    pkg.customPackageType === 'Hajj'
  );
};

// Check if package is Umrah type
const isUmrahPackage = (pkg) => {
  return (
    pkg.packageType === 'Umrah' ||
    pkg.packageType === 'উমরাহ' ||
    pkg.customPackageType === 'Custom Umrah' ||
    pkg.customPackageType === 'Umrah'
  );
};

// Calculate financial summary from packages
const calculateFinancialSummary = (packages = []) => {
  const summary = {
    overall: {
      customers: 0,
      billed: 0,
      paid: 0,
      due: 0,
      costingPrice: 0,
      advance: 0,
      profit: 0,
    },
    hajj: {
      customers: 0,
      billed: 0,
      paid: 0,
      due: 0,
      costingPrice: 0,
      advance: 0,
      profit: 0,
    },
    umrah: {
      customers: 0,
      billed: 0,
      paid: 0,
      due: 0,
      costingPrice: 0,
      advance: 0,
      profit: 0,
    },
  };

  packages.forEach((pkg) => {
    // Calculate assigned customers count
    const assignedCount = Array.isArray(pkg.assignedCustomers)
      ? pkg.assignedCustomers.length
      : 0;

    // Calculate billed amount (package total price)
    const billed = resolveNumber(
      pkg.financialSummary?.totalBilled,
      pkg.financialSummary?.billTotal,
      pkg.financialSummary?.subtotal,
      pkg.paymentSummary?.totalBilled,
      pkg.paymentSummary?.billTotal,
      pkg.totalPrice,
      pkg.totalPriceBdt,
      pkg.totals?.grandTotal,
      pkg.totals?.subtotal,
      pkg.profitLoss?.packagePrice,
      pkg.profitLoss?.totalOriginalPrice
    );

    // Calculate paid amount
    const paid = resolveNumber(
      pkg.financialSummary?.totalPaid,
      pkg.financialSummary?.paidAmount,
      pkg.paymentSummary?.totalPaid,
      pkg.paymentSummary?.paid,
      pkg.payments?.totalPaid,
      pkg.payments?.paid,
      pkg.totalPaid,
      pkg.depositReceived,
      pkg.receivedAmount
    );

    // Calculate due (billed - paid)
    const due = Math.max(billed - paid, 0);

    // Calculate profit/loss
    const profit = calculateProfitLoss(pkg);
    const profitValue = profit.profitValue || 0;
    const costingPrice = profit.costingPrice || 0;

    // Determine package type
    const isHajj = isHajjPackage(pkg);
    const isUmrah = isUmrahPackage(pkg);

    // Add to overall summary
    summary.overall.customers += assignedCount;
    summary.overall.billed += billed;
    summary.overall.paid += paid;
    summary.overall.due += due;
    summary.overall.costingPrice += costingPrice;
    summary.overall.profit += profitValue;

    // Add to type-specific summary
    if (isHajj) {
      summary.hajj.customers += assignedCount;
      summary.hajj.billed += billed;
      summary.hajj.paid += paid;
      summary.hajj.due += due;
      summary.hajj.costingPrice += costingPrice;
      summary.hajj.profit += profitValue;
    } else if (isUmrah) {
      summary.umrah.customers += assignedCount;
      summary.umrah.billed += billed;
      summary.umrah.paid += paid;
      summary.umrah.due += due;
      summary.umrah.costingPrice += costingPrice;
      summary.umrah.profit += profitValue;
    }
  });

  // Calculate advance = paid - costingPrice (can be negative)
  summary.overall.advance = summary.overall.paid - summary.overall.costingPrice;
  summary.hajj.advance = summary.hajj.paid - summary.hajj.costingPrice;
  summary.umrah.advance = summary.umrah.paid - summary.umrah.costingPrice;

  return summary;
};

// Create Agent
app.post("/api/haj-umrah/agents", async (req, res) => {
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

    // Generate unique agent ID
    const agentId = await generateHajUmrahAgentId(db);

    const now = new Date();
    const doc = {
      agentId,
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

// Bulk Create Agents
app.post("/api/haj-umrah/agents/bulk", async (req, res) => {
  try {
    // Support both: body = [...]  or  body = { agents: [...] }
    const agentsPayload = Array.isArray(req.body) ? req.body : req.body?.agents;

    if (!Array.isArray(agentsPayload) || agentsPayload.length === 0) {
      return res.status(400).send({
        error: true,
        message: "agents array is required and should not be empty"
      });
    }

    const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;

    const docs = [];
    for (let i = 0; i < agentsPayload.length; i++) {
      const item = agentsPayload[i] || {};
      const {
        tradeName,
        tradeLocation,
        ownerName,
        contactNo,
        dob,
        nid,
        passport
      } = item;

      // Basic required fields
      if (!tradeName || !tradeLocation || !ownerName || !contactNo) {
        return res.status(400).send({
          error: true,
          message: `Row ${i + 1}: tradeName, tradeLocation, ownerName and contactNo are required`
        });
      }

      // Same validations as single create
      if (!phoneRegex.test(String(contactNo).trim())) {
        return res.status(400).send({
          error: true,
          message: `Row ${i + 1}: Enter a valid phone number`
        });
      }
      if (nid && !/^[0-9]{8,20}$/.test(String(nid).trim())) {
        return res.status(400).send({
          error: true,
          message: `Row ${i + 1}: NID should be 8-20 digits`
        });
      }
      if (passport && !/^[A-Za-z0-9]{6,12}$/.test(String(passport).trim())) {
        return res.status(400).send({
          error: true,
          message: `Row ${i + 1}: Passport should be 6-12 chars`
        });
      }
      if (dob && !isValidDate(dob)) {
        return res.status(400).send({
          error: true,
          message: `Row ${i + 1}: Invalid date format for dob (YYYY-MM-DD)`
        });
      }

      // Generate unique agent ID for each row
      const agentId = await generateHajUmrahAgentId(db);
      const now = new Date();

      docs.push({
        agentId,
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
      });
    }

    const result = await agents.insertMany(docs);
    const insertedIds = result.insertedIds || {};

    const responseData = docs.map((doc, index) => ({
      _id: insertedIds[index] || null,
      ...doc
    }));

    return res.status(201).send({
      success: true,
      message: "Agents created successfully",
      count: responseData.length,
      data: responseData
    });
  } catch (error) {
    console.error('Bulk create agents error:', error);
    res.status(500).json({
      error: true,
      message: "Internal server error while creating agents in bulk"
    });
  }
});

// List Agents (with pagination and search)
app.get("/api/haj-umrah/agents", async (req, res) => {
  try {
    const { page = 1, limit = 50, q } = req.query;
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 50, 1), 20000);

    const filter = { isActive: { $ne: false } };
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

// Get single agent by id - UPDATED VERSION
app.get("/api/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).send({ error: true, message: "Invalid agent id" });
    }

    const agent = await agents.findOne({ _id: new ObjectId(id) });
    if (!agent) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }

    // Fetch all packages for this agent
    const packages = await agentPackages
      .find({ agentId: new ObjectId(id) })
      .toArray();

    // Calculate financial summary from packages
    const financialSummary = calculateFinancialSummary(packages);

    // Initialize due amounts if missing (migration for old agents)
    if (
      agent.totalDue === undefined ||
      agent.hajDue === undefined ||
      agent.umrahDue === undefined
    ) {
      console.log("🔄 Migrating agent to add due amounts:", agent._id);
      const updateDoc = {};
      if (agent.totalDue === undefined) updateDoc.totalDue = 0;
      if (agent.hajDue === undefined) updateDoc.hajDue = 0;
      if (agent.umrahDue === undefined) updateDoc.umrahDue = 0;
      updateDoc.updatedAt = new Date();

      await agents.updateOne({ _id: new ObjectId(id) }, { $set: updateDoc });

      Object.assign(agent, updateDoc);
      console.log("✅ Agent migrated successfully");
    }

    // Add calculated financial summary to agent object
    // Frontend will use these values with pickNumberFromObject function
    const agentWithSummary = {
      ...agent,
      // Overall summary
      totalHaji: financialSummary.overall.customers,
      totalCustomers: financialSummary.overall.customers,
      totalBilled: financialSummary.overall.billed,
      totalBill: financialSummary.overall.billed,
      totalBillAmount: financialSummary.overall.billed,
      totalRevenue: financialSummary.overall.billed,
      totalPaid: financialSummary.overall.paid,
      totalDeposit: financialSummary.overall.paid,
      totalReceived: financialSummary.overall.paid,
      totalCollection: financialSummary.overall.paid,
      totalDue: financialSummary.overall.due,
      totalAdvance: financialSummary.overall.advance,
      totalProfit: financialSummary.overall.profit,
      totalCostingPrice: financialSummary.overall.costingPrice,

      // Hajj summary
      hajCustomers: financialSummary.hajj.customers,
      hajjCustomers: financialSummary.hajj.customers,
      totalHajjCustomers: financialSummary.hajj.customers,
      totalHajCustomers: financialSummary.hajj.customers,
      hajBill: financialSummary.hajj.billed,
      hajjBill: financialSummary.hajj.billed,
      totalHajjBill: financialSummary.hajj.billed,
      hajTotalBill: financialSummary.hajj.billed,
      hajPaid: financialSummary.hajj.paid,
      hajjPaid: financialSummary.hajj.paid,
      hajjDeposit: financialSummary.hajj.paid,
      hajDeposit: financialSummary.hajj.paid,
      totalHajjPaid: financialSummary.hajj.paid,
      hajDue: financialSummary.hajj.due,
      hajAdvance: financialSummary.hajj.advance,
      hajProfit: financialSummary.hajj.profit,
      hajCostingPrice: financialSummary.hajj.costingPrice,

      // Umrah summary
      umrahCustomers: financialSummary.umrah.customers,
      totalUmrahCustomers: financialSummary.umrah.customers,
      totalUmrahHaji: financialSummary.umrah.customers,
      umrahBill: financialSummary.umrah.billed,
      totalUmrahBill: financialSummary.umrah.billed,
      umrahPaid: financialSummary.umrah.paid,
      umrahDeposit: financialSummary.umrah.paid,
      totalUmrahPaid: financialSummary.umrah.paid,
      umrahDue: financialSummary.umrah.due,
      umrahAdvance: financialSummary.umrah.advance,
      umrahProfit: financialSummary.umrah.profit,
      umrahCostingPrice: financialSummary.umrah.costingPrice,

      // Include the full financial summary object for reference
      financialSummary: financialSummary,
    };

    res.send({ success: true, data: agentWithSummary });
  } catch (error) {
    console.error("Get agent error:", error);
    res
      .status(500)
      .json({
        error: true,
        message: "Internal server error while fetching agent",
      });
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

// DELETE /api/haj-umrah/agents/:id
app.delete("/api/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: "Invalid agent id"
      });
    }

    const agent = await agents.findOne({ 
      _id: new ObjectId(id)
    });

    if (!agent) {
      return res.status(404).json({
        success: false,
        message: "Haj-Umrah Agent not found"
      });
    }

    // Soft delete
    await agents.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    res.json({
      success: true,
      message: "Haj-Umrah Agent deleted successfully"
    });

  } catch (error) {
    console.error('Delete haj-umrah agent error:', error);
    res.status(500).json({
      success: false,
      message: "Failed to delete haj-umrah agent",
      error: error.message
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

    // Calculate Profit and Loss
    // Package Price = যে price agent দেবে (totalPrice)
    // Costing Price = যে price খরচ হয়েছে (totals.grandTotal)
    const packagePrice = parseFloat(package.totalPrice) || 0;
    const costingPrice = parseFloat(package.totals?.grandTotal) || 0;
    const profitLoss = packagePrice - costingPrice;
    
    // Add profit/loss information to package
    package.profitLoss = {
      packagePrice: packagePrice,
      costingPrice: costingPrice,
      profitLoss: profitLoss,
      isProfit: profitLoss > 0,
      isLoss: profitLoss < 0,
      profitLossPercentage: packagePrice > 0 ? ((profitLoss / packagePrice) * 100).toFixed(2) : 0
    };

    // Calculate payment summary (total paid & remaining due) for this package
    const agentId = package.agentId || package.agent?._id || package.agent?._id?.toString();
    let paymentSummary = {
      totalPaid: 0,
      remainingDue: parseFloat(package.totals?.grandTotal ?? package.totalPrice ?? 0) || 0
    };

    if (agentId) {
      const agentIdStr = String(agentId);
      const packageIdCandidates = [String(id)];
      if (ObjectId.isValid(id)) {
        packageIdCandidates.push(new ObjectId(id));
      }

      const filter = {
        isActive: { $ne: false },
        partyType: 'agent',
        'meta.packageId': { $in: packageIdCandidates }
      };

      const orConditions = [{ partyId: agentIdStr }];
      if (ObjectId.isValid(agentIdStr)) {
        orConditions.push({ partyId: new ObjectId(agentIdStr) });
      }
      filter.$or = orConditions;

      const totalsAgg = await transactions
        .aggregate([
          { $match: filter },
          {
            $group: {
              _id: null,
              totalCredit: {
                $sum: {
                  $cond: [{ $eq: ['$transactionType', 'credit'] }, '$amount', 0]
                }
              }
            }
          }
        ])
        .toArray();

      const totalPaid = totalsAgg?.[0]?.totalCredit || 0;
      const packageTotal = parseFloat(package.totals?.grandTotal ?? package.totalPrice ?? 0) || 0;
      paymentSummary = {
        totalPaid,
        remainingDue: packageTotal - totalPaid
      };
    }

    package.paymentSummary = paymentSummary;

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
// Update agent package costing
app.put('/api/haj-umrah/agent-packages/:id', async (req, res) => {
  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Check if package exists
    const existingPackage = await agentPackages.findOne({ _id: new ObjectId(id) });
    if (!existingPackage) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const {
      sarToBdtRate,
      discount,
      costs,
      bangladeshVisaPassengers,
      bangladeshAirfarePassengers,
      bangladeshBusPassengers,
      bangladeshTrainingOtherPassengers,
      saudiVisaPassengers,
      saudiMakkahHotelPassengers,
      saudiMadinaHotelPassengers,
      saudiMakkahFoodPassengers,
      saudiMadinaFoodPassengers,
      saudiMakkahZiyaraPassengers,
      saudiMadinaZiyaraPassengers,
      saudiTransportPassengers,
      saudiCampFeePassengers,
      saudiAlMashayerPassengers,
      saudiOthersPassengers,
      totals
    } = req.body;

    // Get the grand total from the payload
    const packageTotal = totals?.grandTotal || existingPackage.totalPrice || 0;

    // Prepare update object
    const updateData = {
      updatedAt: new Date()
    };

    // Update costing fields if provided
    if (sarToBdtRate !== undefined) {
      updateData.sarToBdtRate = parseFloat(sarToBdtRate) || 1;
    }

    if (discount !== undefined) {
      updateData.discount = parseFloat(discount) || 0;
    }

    if (costs !== undefined) {
      updateData.costs = costs || {};
    }

    if (totals !== undefined) {
      updateData.totals = totals || {};
    }

    // Update passenger arrays if provided
    if (bangladeshVisaPassengers !== undefined) {
      updateData.bangladeshVisaPassengers = bangladeshVisaPassengers || [];
    }
    if (bangladeshAirfarePassengers !== undefined) {
      updateData.bangladeshAirfarePassengers = bangladeshAirfarePassengers || [];
    }
    if (bangladeshBusPassengers !== undefined) {
      updateData.bangladeshBusPassengers = bangladeshBusPassengers || [];
    }
    if (bangladeshTrainingOtherPassengers !== undefined) {
      updateData.bangladeshTrainingOtherPassengers = bangladeshTrainingOtherPassengers || [];
    }
    if (saudiVisaPassengers !== undefined) {
      updateData.saudiVisaPassengers = saudiVisaPassengers || [];
    }
    if (saudiMakkahHotelPassengers !== undefined) {
      updateData.saudiMakkahHotelPassengers = saudiMakkahHotelPassengers || [];
    }
    if (saudiMadinaHotelPassengers !== undefined) {
      updateData.saudiMadinaHotelPassengers = saudiMadinaHotelPassengers || [];
    }
    if (saudiMakkahFoodPassengers !== undefined) {
      updateData.saudiMakkahFoodPassengers = saudiMakkahFoodPassengers || [];
    }
    if (saudiMadinaFoodPassengers !== undefined) {
      updateData.saudiMadinaFoodPassengers = saudiMadinaFoodPassengers || [];
    }
    if (saudiMakkahZiyaraPassengers !== undefined) {
      updateData.saudiMakkahZiyaraPassengers = saudiMakkahZiyaraPassengers || [];
    }
    if (saudiMadinaZiyaraPassengers !== undefined) {
      updateData.saudiMadinaZiyaraPassengers = saudiMadinaZiyaraPassengers || [];
    }
    if (saudiTransportPassengers !== undefined) {
      updateData.saudiTransportPassengers = saudiTransportPassengers || [];
    }
    if (saudiCampFeePassengers !== undefined) {
      updateData.saudiCampFeePassengers = saudiCampFeePassengers || [];
    }
    if (saudiAlMashayerPassengers !== undefined) {
      updateData.saudiAlMashayerPassengers = saudiAlMashayerPassengers || [];
    }
    if (saudiOthersPassengers !== undefined) {
      updateData.saudiOthersPassengers = saudiOthersPassengers || [];
    }

    // Update total price based on grand total
    updateData.totalPrice = packageTotal;

    // Update the package
    await agentPackages.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    // Get the agent ID from the existing package
    const agentId = existingPackage.agentId;

    // Recalculate all due amounts from all packages for this agent
    const allPackages = await agentPackages.find({ agentId: agentId }).toArray();

    let calculatedTotal = 0;
    let calculatedHajj = 0;
    let calculatedUmrah = 0;

    // Calculate totals from all packages
    for (const pkg of allPackages) {
      const pkgType = (pkg.customPackageType || pkg.packageType || 'Regular').toLowerCase();
      const pkgTotal = pkg.totalPrice || 0;
      const isPkgHajj = pkgType.includes('haj') || pkgType.includes('hajj');
      const isPkgUmrah = pkgType.includes('umrah');

      calculatedTotal += pkgTotal;
      if (isPkgHajj) calculatedHajj += pkgTotal;
      if (isPkgUmrah) calculatedUmrah += pkgTotal;
    }

    console.log('Recalculated Due Amounts after update:', {
      total: calculatedTotal,
      haj: calculatedHajj,
      umrah: calculatedUmrah
    });

    // Update agent due amounts
    await agents.updateOne(
      { _id: agentId },
      {
        $set: {
          totalDue: calculatedTotal,
          hajDue: calculatedHajj,
          umrahDue: calculatedUmrah,
          updatedAt: new Date()
        }
      }
    );

    // Fetch the updated package with agent details
    const updatedPackage = await agentPackages.findOne({ _id: new ObjectId(id) });
    const updatedAgent = await agents.findOne({ _id: agentId });

    res.json({
      success: true,
      message: 'Package costing updated successfully',
      data: {
        ...updatedPackage,
        agent: updatedAgent
      }
    });
  } catch (error) {
    console.error('Update package costing error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to update package costing',
      error: error.message
    });
  }
});

// POST /api/haj-umrah/agent-packages/:id/costing
// Add/Update agent package costing
app.post('/api/haj-umrah/agent-packages/:id/costing', async (req, res) => {
  try {
    const { id } = req.params;
    
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Check if package exists
    const existingPackage = await agentPackages.findOne({ _id: new ObjectId(id) });
    if (!existingPackage) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const {
      sarToBdtRate,
      discount,
      costs,
      bangladeshVisaPassengers,
      bangladeshAirfarePassengers,
      bangladeshBusPassengers,
      bangladeshTrainingOtherPassengers,
      saudiVisaPassengers,
      saudiMakkahHotelPassengers,
      saudiMadinaHotelPassengers,
      saudiMakkahFoodPassengers,
      saudiMadinaFoodPassengers,
      saudiMakkahZiyaraPassengers,
      saudiMadinaZiyaraPassengers,
      saudiTransportPassengers,
      saudiCampFeePassengers,
      saudiAlMashayerPassengers,
      saudiOthersPassengers,
      totals
    } = req.body;

    // Prepare update object
    const updateData = {
      updatedAt: new Date()
    };

    // Update costing fields if provided
    if (sarToBdtRate !== undefined) {
      updateData.sarToBdtRate = parseFloat(sarToBdtRate) || 1;
    }

    if (discount !== undefined) {
      updateData.discount = parseFloat(discount) || 0;
    }

    if (costs !== undefined) {
      updateData.costs = costs || {};
    }

    if (totals !== undefined) {
      updateData.totals = totals || {};
    }

    // Update passenger arrays if provided
    if (bangladeshVisaPassengers !== undefined) {
      updateData.bangladeshVisaPassengers = bangladeshVisaPassengers || [];
    }
    if (bangladeshAirfarePassengers !== undefined) {
      updateData.bangladeshAirfarePassengers = bangladeshAirfarePassengers || [];
    }
    if (bangladeshBusPassengers !== undefined) {
      updateData.bangladeshBusPassengers = bangladeshBusPassengers || [];
    }
    if (bangladeshTrainingOtherPassengers !== undefined) {
      updateData.bangladeshTrainingOtherPassengers = bangladeshTrainingOtherPassengers || [];
    }
    if (saudiVisaPassengers !== undefined) {
      updateData.saudiVisaPassengers = saudiVisaPassengers || [];
    }
    if (saudiMakkahHotelPassengers !== undefined) {
      updateData.saudiMakkahHotelPassengers = saudiMakkahHotelPassengers || [];
    }
    if (saudiMadinaHotelPassengers !== undefined) {
      updateData.saudiMadinaHotelPassengers = saudiMadinaHotelPassengers || [];
    }
    if (saudiMakkahFoodPassengers !== undefined) {
      updateData.saudiMakkahFoodPassengers = saudiMakkahFoodPassengers || [];
    }
    if (saudiMadinaFoodPassengers !== undefined) {
      updateData.saudiMadinaFoodPassengers = saudiMadinaFoodPassengers || [];
    }
    if (saudiMakkahZiyaraPassengers !== undefined) {
      updateData.saudiMakkahZiyaraPassengers = saudiMakkahZiyaraPassengers || [];
    }
    if (saudiMadinaZiyaraPassengers !== undefined) {
      updateData.saudiMadinaZiyaraPassengers = saudiMadinaZiyaraPassengers || [];
    }
    if (saudiTransportPassengers !== undefined) {
      updateData.saudiTransportPassengers = saudiTransportPassengers || [];
    }
    if (saudiCampFeePassengers !== undefined) {
      updateData.saudiCampFeePassengers = saudiCampFeePassengers || [];
    }
    if (saudiAlMashayerPassengers !== undefined) {
      updateData.saudiAlMashayerPassengers = saudiAlMashayerPassengers || [];
    }
    if (saudiOthersPassengers !== undefined) {
      updateData.saudiOthersPassengers = saudiOthersPassengers || [];
    }

    // IMPORTANT: Do NOT update totalPrice here
    // totalPrice = Agent যে price দেবে (original price, set during package creation)
    // totals.grandTotal = Actual costing price (costingPrice)
    // Profit/Loss = totalPrice - totals.grandTotal

    // Update the package
    await agentPackages.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    // Get the agent ID from the existing package
    const agentId = existingPackage.agentId;

    // Recalculate all due amounts from all packages for this agent
    const allPackages = await agentPackages.find({ agentId: agentId }).toArray();

    let calculatedTotal = 0;
    let calculatedHajj = 0;
    let calculatedUmrah = 0;

    // Calculate totals from all packages
    for (const pkg of allPackages) {
      const pkgType = (pkg.customPackageType || pkg.packageType || 'Regular').toLowerCase();
      const pkgTotal = pkg.totalPrice || 0;
      const isPkgHajj = pkgType.includes('haj') || pkgType.includes('hajj');
      const isPkgUmrah = pkgType.includes('umrah');

      calculatedTotal += pkgTotal;
      if (isPkgHajj) calculatedHajj += pkgTotal;
      if (isPkgUmrah) calculatedUmrah += pkgTotal;
    }

    console.log('Recalculated Due Amounts after costing update:', {
      total: calculatedTotal,
      haj: calculatedHajj,
      umrah: calculatedUmrah
    });

    // Update agent due amounts
    await agents.updateOne(
      { _id: agentId },
      {
        $set: {
          totalDue: calculatedTotal,
          hajDue: calculatedHajj,
          umrahDue: calculatedUmrah,
          updatedAt: new Date()
        }
      }
    );

    // Fetch the updated package with agent details
    const updatedPackage = await agentPackages.findOne({ _id: new ObjectId(id) });
    const updatedAgent = await agents.findOne({ _id: agentId });

    res.status(200).json({
      success: true,
      message: 'Package costing added/updated successfully',
      data: {
        ...updatedPackage,
        agent: updatedAgent
      }
    });
  } catch (error) {
    console.error('Add costing error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to add/update package costing',
      error: error.message
    });
  }
});

// GET /api/haj-umrah/agent-packages/:id/transactions
// Fetch transaction history for a package's agent
app.get('/api/haj-umrah/agent-packages/:id/transactions', async (req, res) => {
  try {
    const { id } = req.params;
    const { fromDate, toDate, page = 1, limit = 20 } = req.query || {};

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    const packageDoc = await agentPackages.findOne({ _id: new ObjectId(id) });
    if (!packageDoc) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const agentId = packageDoc.agentId || packageDoc.agent?._id || packageDoc.agent?._id?.toString();
    if (!agentId) {
      return res.status(400).json({
        success: false,
        message: 'Package does not have an associated agent'
      });
    }

    const agentIdStr = String(agentId);
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    const packageIdCandidates = [String(id)];
    if (ObjectId.isValid(id)) {
      packageIdCandidates.push(new ObjectId(id));
    }

    const filter = {
      isActive: { $ne: false },
      partyType: 'agent',
      'meta.packageId': { $in: packageIdCandidates }
    };

    const orConditions = [{ partyId: agentIdStr }];
    if (ObjectId.isValid(agentIdStr)) {
      orConditions.push({ partyId: new ObjectId(agentIdStr) });
    }
    filter.$or = orConditions;

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

    const cursor = transactions
      .find(filter)
      .sort({ date: -1, createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum);

    const totalsPipeline = [
      { $match: filter },
      {
        $group: {
          _id: null,
          totalCredit: {
            $sum: {
              $cond: [{ $eq: ['$transactionType', 'credit'] }, '$amount', 0]
            }
          },
          totalDebit: {
            $sum: {
              $cond: [{ $eq: ['$transactionType', 'debit'] }, '$amount', 0]
            }
          },
          lastPaymentDate: {
            $max: {
              $cond: [{ $eq: ['$transactionType', 'credit'] }, '$date', null]
            }
          }
        }
      }
    ];

    const [items, total, totalsAgg] = await Promise.all([
      cursor.toArray(),
      transactions.countDocuments(filter),
      transactions.aggregate(totalsPipeline).toArray()
    ]);

    const totals = {
      totalCredit: totalsAgg?.[0]?.totalCredit || 0,
      totalDebit: totalsAgg?.[0]?.totalDebit || 0,
      net: (totalsAgg?.[0]?.totalCredit || 0) - (totalsAgg?.[0]?.totalDebit || 0),
      lastPaymentDate: totalsAgg?.[0]?.lastPaymentDate || null
    };

    const packageTotal = packageDoc?.totals?.grandTotal ?? packageDoc?.totalPrice ?? 0;
    const totalPaid = totals.totalCredit;
    const remainingDue = packageTotal - totalPaid;
    const agentDoc = agentIdStr && ObjectId.isValid(agentIdStr) ? await agents.findOne({ _id: new ObjectId(agentIdStr) }) : null;

    res.json({
      success: true,
      data: items,
      agent: {
        id: agentIdStr,
        name: packageDoc.agentName || packageDoc.agent?.name || agentDoc?.name || null,
        phone: agentDoc?.phone || agentDoc?.contactNo || null
      },
      package: {
        id: String(packageDoc._id),
        name: packageDoc.packageName || null,
        totalPrice: packageTotal,
        totalPaid,
        remainingDue
      },
      totals,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get agent package transactions error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch transaction history',
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
    const packagePrice = parseFloat(package.totalPrice) || 0;
    const packageType = (package.packageType || package.customPackageType || '').toLowerCase();
    const isHajjPackage = packageType.includes('haj') || packageType.includes('hajj');
    const isUmrahPackage = packageType.includes('umrah');

    // Log package info for debugging
    console.log('Assigning customers to package:', {
      packageId: id,
      packageName: package.packageName,
      packagePrice,
      packageType,
      isHajjPackage,
      isUmrahPackage
    });

    // Handle array of customer IDs
    if (customerIds && Array.isArray(customerIds)) {
      const existingIds = existingCustomers.map(c => c._id?.toString() || c.toString());
      const newIds = customerIds
        .map(id => new ObjectId(id))
        .filter(id => !existingIds.includes(id.toString()));
      
      if (newIds.length === 0) {
        return res.json({
          success: true,
          message: 'All customers are already assigned to this package'
        });
      }

      // Update customer profiles with package amount
      let updatedCount = 0;
      let notFoundCount = 0;
      const updateErrors = [];

      for (const customerId of newIds) {
        try {
          const customerIdStr = customerId.toString();
          let customerUpdated = false;

          // Try to find in haji collection (check both _id and customerId field)
          const hajiCustomer = await haji.findOne({ 
            $or: [
              { _id: customerId },
              { customerId: customerIdStr }
            ],
            isActive: { $ne: false }
          });
          
          if (hajiCustomer) {
            const currentTotal = parseFloat(hajiCustomer.totalAmount || hajiCustomer.familyTotal || 0);
            const newTotal = currentTotal + packagePrice;
            await haji.updateOne(
              { _id: hajiCustomer._id },
              {
                $set: {
                  totalAmount: newTotal,
                  packageInfo: {
                    packageId: new ObjectId(id),
                    packageName: package.packageName,
                    packageType: package.packageType || 'Regular',
                    customPackageType: package.customPackageType || '',
                    agentId: package.agentId,
                    assignedAt: new Date()
                  },
                  updatedAt: new Date()
                }
              }
            );
              console.log(`Updated Haji customer ${customerIdStr} with amount ${packagePrice}, new total: ${newTotal}`);
              customerUpdated = true;
              updatedCount++;
          }

          // Try to find in umrah collection (check both _id and customerId field)
          if (!customerUpdated) {
            const umrahCustomer = await umrah.findOne({ 
              $or: [
                { _id: customerId },
                { customerId: customerIdStr }
              ],
              isActive: { $ne: false }
            });
            
            if (umrahCustomer) {
              const currentTotal = parseFloat(umrahCustomer.totalAmount || umrahCustomer.familyTotal || 0);
              const newTotal = currentTotal + packagePrice;
              await umrah.updateOne(
                { _id: umrahCustomer._id },
                {
                  $set: {
                    totalAmount: newTotal,
                    packageInfo: {
                      packageId: new ObjectId(id),
                      packageName: package.packageName,
                      packageType: package.packageType || 'Regular',
                      customPackageType: package.customPackageType || '',
                      agentId: package.agentId,
                      assignedAt: new Date()
                    },
                    updatedAt: new Date()
                  }
                }
              );
              console.log(`Updated Umrah customer ${customerIdStr} with amount ${packagePrice}, new total: ${newTotal}`);
              customerUpdated = true;
              updatedCount++;
            }
          }

          // Try to find in airCustomers collection
          if (!customerUpdated) {
            const airCustomer = await airCustomers.findOne({ 
              $or: [
                { _id: customerId },
                { customerId: customerIdStr }
              ],
              isActive: { $ne: false }
            });
            
            if (airCustomer) {
              const currentTotal = parseFloat(airCustomer.totalAmount || 0);
              const newTotal = currentTotal + packagePrice;
              await airCustomers.updateOne(
                { _id: airCustomer._id },
                {
                  $set: {
                    totalAmount: newTotal,
                    updatedAt: new Date()
                  }
                }
              );
              console.log(`Updated AirCustomer ${customerIdStr} with amount ${packagePrice}, new total: ${newTotal}`);
              customerUpdated = true;
              updatedCount++;
            }
          }

          if (!customerUpdated) {
            console.warn(`Customer ${customerIdStr} not found in any collection (haji, umrah, airCustomers)`);
            notFoundCount++;
          }
        } catch (err) {
          console.error(`Error updating customer ${customerId}:`, err);
          console.error('Error stack:', err.stack);
          updateErrors.push({
            customerId: customerId.toString(),
            error: err.message
          });
          // Continue with other customers even if one fails
        }
      }

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

      const responseMessage = updatedCount > 0 
        ? `${newIds.length} customers assigned, ${updatedCount} profiles updated with package amount`
        : `${newIds.length} customers assigned, but ${notFoundCount} customer profiles not found`;

      return res.json({
        success: true,
        message: responseMessage,
        data: {
          assigned: newIds.length,
          profilesUpdated: updatedCount,
          notFound: notFoundCount,
          packagePrice,
          errors: updateErrors.length > 0 ? updateErrors : undefined
        }
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

    // Find the customer being removed to update their profile
    const customerToRemove = (package.assignedCustomers || []).find(
      c => c._id?.toString() === customerId || c.toString() === customerId
    );

    const packagePrice = parseFloat(package.totalPrice) || 0;
    const packageType = (package.packageType || package.customPackageType || '').toLowerCase();
    const isHajjPackage = packageType.includes('haj') || packageType.includes('hajj');
    const isUmrahPackage = packageType.includes('umrah');

    // Update customer profile by subtracting package amount
    if (customerToRemove && packagePrice > 0) {
      try {
        const customerObjId = ObjectId.isValid(customerId) ? new ObjectId(customerId) : null;
        
        // Try to find in haji collection
        if (isHajjPackage && customerObjId) {
          const hajiCustomer = await haji.findOne({ 
            $or: [{ _id: customerObjId }, { customerId: customerId }] 
          });
          if (hajiCustomer) {
            const currentTotal = parseFloat(hajiCustomer.totalAmount || 0);
            const newTotal = Math.max(0, currentTotal - packagePrice);
            await haji.updateOne(
              { _id: hajiCustomer._id },
              {
                $set: {
                  totalAmount: newTotal,
                  updatedAt: new Date()
                },
                $unset: {
                  packageInfo: ""
                }
              }
            );
          }
        }

        // Try to find in umrah collection
        if (isUmrahPackage && customerObjId) {
          const umrahCustomer = await umrah.findOne({ 
            $or: [{ _id: customerObjId }, { customerId: customerId }] 
          });
          if (umrahCustomer) {
            const currentTotal = parseFloat(umrahCustomer.totalAmount || 0);
            const newTotal = Math.max(0, currentTotal - packagePrice);
            await umrah.updateOne(
              { _id: umrahCustomer._id },
              {
                $set: {
                  totalAmount: newTotal,
                  updatedAt: new Date()
                },
                $unset: {
                  packageInfo: ""
                }
              }
            );
          }
        }

        // Try to find in airCustomers collection
        if (customerObjId) {
          const airCustomer = await airCustomers.findOne({ 
            $or: [{ _id: customerObjId }, { customerId: customerId }] 
          });
          if (airCustomer) {
            const currentTotal = parseFloat(airCustomer.totalAmount || 0);
            const newTotal = Math.max(0, currentTotal - packagePrice);
            await airCustomers.updateOne(
              { _id: airCustomer._id },
              {
                $set: {
                  totalAmount: newTotal,
                  updatedAt: new Date()
                }
              }
            );
          }
        }
      } catch (err) {
        console.error(`Error updating customer ${customerId} on removal:`, err);
        // Continue with removal even if update fails
      }
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

    const filter = { isActive: { $ne: false } };
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
      status,
      totalPrice // Main sale price - set during creation, never updated by costing endpoint
    } = req.body;

    // Validation
    if (!packageName || !packageYear) {
      return res.status(400).json({
        success: false,
        message: 'Package name and year are required'
      });
    }

    // Auto-generate package name with serial number
    // Find existing packages with similar base name pattern (baseName + " " + number)
    const basePackageName = String(packageName).trim();
    
    // Find all packages that start with the base name and have a number suffix
    // Pattern: "Base Name 01", "Base Name 02", etc.
    const existingPackages = await packages.find({
      packageName: { $regex: new RegExp(`^${basePackageName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\s+\\d+$`, 'i') }
    }).toArray();

    let nextSerialNumber = 1; // Start from 01

    if (existingPackages.length > 0) {
      // Extract numbers from existing package names and find the highest
      const numbers = existingPackages
        .map(pkg => {
          const match = pkg.packageName.match(/(\d+)$/);
          return match ? parseInt(match[1], 10) : 0;
        })
        .filter(num => num > 0);

      if (numbers.length > 0) {
        const maxNumber = Math.max(...numbers);
        nextSerialNumber = maxNumber + 1;
      }
    }

    // Format serial number with leading zero (01, 02, ..., 99, 100, etc.)
    const serialSuffix = String(nextSerialNumber).padStart(2, '0');
    const finalPackageName = `${basePackageName} ${serialSuffix}`;

    // Ensure totals.passengerTotals structure exists
    // Passenger totals are stored separately: adult, child, infant
    const totalsData = totals || {};
    if (!totalsData.passengerTotals) {
      totalsData.passengerTotals = {
        adult: 0,
        child: 0,
        infant: 0
      };
    } else {
      // Ensure all three passenger types are present and properly formatted
      totalsData.passengerTotals = {
        adult: Number((parseFloat(totalsData.passengerTotals.adult) || 0).toFixed(2)),
        child: Number((parseFloat(totalsData.passengerTotals.child) || 0).toFixed(2)),
        infant: Number((parseFloat(totalsData.passengerTotals.infant) || 0).toFixed(2))
      };
    }

    // Normalize costs object - ensure campFee is a number
    const normalizedCosts = costs || {};
    if (normalizedCosts.campFee !== undefined) {
      normalizedCosts.campFee = Number((parseFloat(normalizedCosts.campFee) || 0).toFixed(2));
    }

    // Create package document
    // Important: totalPrice is optional and should be set separately if needed.
    // It should NEVER be updated by the costing endpoint (/packages/:id/costing).
    // The costing endpoint only updates costs, totals.grandTotal, etc.
    // Passenger totals (adult, child, infant) are stored separately in totals.passengerTotals
    const packageDoc = {
      packageName: finalPackageName,
      packageYear: String(packageYear),
      packageMonth: packageMonth || '',
      packageType: packageType || 'Regular',
      customPackageType: customPackageType || '',
      sarToBdtRate: parseFloat(sarToBdtRate) || 0,
      notes: notes || '',
      status: status || 'Active',
      totalPrice: totalPrice !== undefined && totalPrice !== null && totalPrice !== '' 
        ? Number((parseFloat(totalPrice) || 0).toFixed(2)) 
        : 0, // Optional: only set if provided
      costs: normalizedCosts,
      totals: totalsData, // Contains passengerTotals with adult, child, infant separately
      assignedPassengerCounts: {
        adult: 0,
        child: 0,
        infant: 0
      }, // Track assigned passenger counts for profit/loss calculation
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await packages.insertOne(packageDoc);
    const createdPackage = await packages.findOne({ _id: result.insertedId });

    // Calculate Profit/Loss for the response
    const assignedCounts = createdPackage.assignedPassengerCounts || {
      adult: 0,
      child: 0,
      infant: 0
    };

    // Get original prices (from totals.passengerTotals - set during package creation)
    const originalPrices = {
      adult: parseFloat(createdPackage.totals?.passengerTotals?.adult) || 0,
      child: parseFloat(createdPackage.totals?.passengerTotals?.child) || 0,
      infant: parseFloat(createdPackage.totals?.passengerTotals?.infant) || 0
    };

    // Get costing prices (from totals.costingPassengerTotals - set during costing)
    const costingPrices = {
      adult: parseFloat(createdPackage.totals?.costingPassengerTotals?.adult) || 0,
      child: parseFloat(createdPackage.totals?.costingPassengerTotals?.child) || 0,
      infant: parseFloat(createdPackage.totals?.costingPassengerTotals?.infant) || 0
    };

    // Calculate Total Original Price based on assigned passengers
    const totalOriginalPrice = 
      (assignedCounts.adult * originalPrices.adult) +
      (assignedCounts.child * originalPrices.child) +
      (assignedCounts.infant * originalPrices.infant);

    // Calculate Total Costing Price based on assigned passengers
    const totalCostingPrice = 
      (assignedCounts.adult * costingPrices.adult) +
      (assignedCounts.child * costingPrices.child) +
      (assignedCounts.infant * costingPrices.infant);

    // Calculate Profit/Loss
    const profitOrLoss = totalOriginalPrice - totalCostingPrice;

    // Calculate per-type totals for display
    const passengerOriginalTotals = {
      adult: assignedCounts.adult * originalPrices.adult,
      child: assignedCounts.child * originalPrices.child,
      infant: assignedCounts.infant * originalPrices.infant
    };

    const passengerCostingTotals = {
      adult: assignedCounts.adult * costingPrices.adult,
      child: assignedCounts.child * costingPrices.child,
      infant: assignedCounts.infant * costingPrices.infant
    };

    const passengerProfit = {
      adult: passengerOriginalTotals.adult - passengerCostingTotals.adult,
      child: passengerOriginalTotals.child - passengerCostingTotals.child,
      infant: passengerOriginalTotals.infant - passengerCostingTotals.infant
    };

    res.status(201).json({
      success: true,
      message: 'Package created successfully',
      data: {
        ...createdPackage,
        assignedPassengerCounts: assignedCounts,
        totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
        totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
        profitOrLoss: Number(profitOrLoss.toFixed(2)),
        passengerOriginalTotals: {
          adult: Number(passengerOriginalTotals.adult.toFixed(2)),
          child: Number(passengerOriginalTotals.child.toFixed(2)),
          infant: Number(passengerOriginalTotals.infant.toFixed(2))
        },
        passengerCostingTotals: {
          adult: Number(passengerCostingTotals.adult.toFixed(2)),
          child: Number(passengerCostingTotals.child.toFixed(2)),
          infant: Number(passengerCostingTotals.infant.toFixed(2))
        },
        passengerProfit: {
          adult: Number(passengerProfit.adult.toFixed(2)),
          child: Number(passengerProfit.child.toFixed(2)),
          infant: Number(passengerProfit.infant.toFixed(2))
        },
        profitLoss: {
          assignedPassengerCounts: assignedCounts,
          originalPrices: originalPrices,
          costingPrices: costingPrices,
          totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
          totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
          profitOrLoss: Number(profitOrLoss.toFixed(2)),
          passengerOriginalTotals: {
            adult: Number(passengerOriginalTotals.adult.toFixed(2)),
            child: Number(passengerOriginalTotals.child.toFixed(2)),
            infant: Number(passengerOriginalTotals.infant.toFixed(2))
          },
          passengerCostingTotals: {
            adult: Number(passengerCostingTotals.adult.toFixed(2)),
            child: Number(passengerCostingTotals.child.toFixed(2)),
            infant: Number(passengerCostingTotals.infant.toFixed(2))
          },
          passengerProfit: {
            adult: Number(passengerProfit.adult.toFixed(2)),
            child: Number(passengerProfit.child.toFixed(2)),
            infant: Number(passengerProfit.infant.toFixed(2))
          }
        }
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
      // Ensure assignedPassengerCounts exists
      const assignedCounts = pkg.assignedPassengerCounts || {
        adult: 0,
        child: 0,
        infant: 0
      };

      // Get original prices (from totals.passengerTotals - set during package creation)
      const originalPrices = {
        adult: parseFloat(pkg.totals?.passengerTotals?.adult) || 0,
        child: parseFloat(pkg.totals?.passengerTotals?.child) || 0,
        infant: parseFloat(pkg.totals?.passengerTotals?.infant) || 0
      };

      // Get costing prices (from totals.costingPassengerTotals - set during costing)
      const costingPrices = {
        adult: parseFloat(pkg.totals?.costingPassengerTotals?.adult) || 0,
        child: parseFloat(pkg.totals?.costingPassengerTotals?.child) || 0,
        infant: parseFloat(pkg.totals?.costingPassengerTotals?.infant) || 0
      };

      // Calculate Total Original Price based on assigned passengers
      const totalOriginalPrice = 
        (assignedCounts.adult * originalPrices.adult) +
        (assignedCounts.child * originalPrices.child) +
        (assignedCounts.infant * originalPrices.infant);

      // Calculate Total Costing Price based on assigned passengers
      const totalCostingPrice = 
        (assignedCounts.adult * costingPrices.adult) +
        (assignedCounts.child * costingPrices.child) +
        (assignedCounts.infant * costingPrices.infant);

      // Calculate Profit/Loss
      const profitOrLoss = totalOriginalPrice - totalCostingPrice;

      // Calculate per-type totals for display
      const passengerOriginalTotals = {
        adult: assignedCounts.adult * originalPrices.adult,
        child: assignedCounts.child * originalPrices.child,
        infant: assignedCounts.infant * originalPrices.infant
      };

      const passengerCostingTotals = {
        adult: assignedCounts.adult * costingPrices.adult,
        child: assignedCounts.child * costingPrices.child,
        infant: assignedCounts.infant * costingPrices.infant
      };

      const passengerProfit = {
        adult: passengerOriginalTotals.adult - passengerCostingTotals.adult,
        child: passengerOriginalTotals.child - passengerCostingTotals.child,
        infant: passengerOriginalTotals.infant - passengerCostingTotals.infant
      };

      return {
        ...pkg,
        assignedPassengerCounts: assignedCounts,
        totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
        totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
        profitOrLoss: Number(profitOrLoss.toFixed(2)),
        passengerOriginalTotals: {
          adult: Number(passengerOriginalTotals.adult.toFixed(2)),
          child: Number(passengerOriginalTotals.child.toFixed(2)),
          infant: Number(passengerOriginalTotals.infant.toFixed(2))
        },
        passengerCostingTotals: {
          adult: Number(passengerCostingTotals.adult.toFixed(2)),
          child: Number(passengerCostingTotals.child.toFixed(2)),
          infant: Number(passengerCostingTotals.infant.toFixed(2))
        },
        passengerProfit: {
          adult: Number(passengerProfit.adult.toFixed(2)),
          child: Number(passengerProfit.child.toFixed(2)),
          infant: Number(passengerProfit.infant.toFixed(2))
        }
      };
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

    // Ensure assignedPassengerCounts exists
    const assignedCounts = package.assignedPassengerCounts || {
      adult: 0,
      child: 0,
      infant: 0
    };

    // Get original prices (from totals.passengerTotals - set during package creation)
    const originalPrices = {
      adult: parseFloat(package.totals?.passengerTotals?.adult) || 0,
      child: parseFloat(package.totals?.passengerTotals?.child) || 0,
      infant: parseFloat(package.totals?.passengerTotals?.infant) || 0
    };

    // Get costing prices (from totals.costingPassengerTotals - set during costing)
    const costingPrices = {
      adult: parseFloat(package.totals?.costingPassengerTotals?.adult) || 0,
      child: parseFloat(package.totals?.costingPassengerTotals?.child) || 0,
      infant: parseFloat(package.totals?.costingPassengerTotals?.infant) || 0
    };

    // Calculate Total Original Price based on assigned passengers
    const totalOriginalPrice = 
      (assignedCounts.adult * originalPrices.adult) +
      (assignedCounts.child * originalPrices.child) +
      (assignedCounts.infant * originalPrices.infant);

    // Calculate Total Costing Price based on assigned passengers
    const totalCostingPrice = 
      (assignedCounts.adult * costingPrices.adult) +
      (assignedCounts.child * costingPrices.child) +
      (assignedCounts.infant * costingPrices.infant);

    // Calculate Profit/Loss
    const profitOrLoss = totalOriginalPrice - totalCostingPrice;

    // Calculate per-type totals for display
    const passengerOriginalTotals = {
      adult: assignedCounts.adult * originalPrices.adult,
      child: assignedCounts.child * originalPrices.child,
      infant: assignedCounts.infant * originalPrices.infant
    };

    const passengerCostingTotals = {
      adult: assignedCounts.adult * costingPrices.adult,
      child: assignedCounts.child * costingPrices.child,
      infant: assignedCounts.infant * costingPrices.infant
    };

    const passengerProfit = {
      adult: passengerOriginalTotals.adult - passengerCostingTotals.adult,
      child: passengerOriginalTotals.child - passengerCostingTotals.child,
      infant: passengerOriginalTotals.infant - passengerCostingTotals.infant
    };

    // Remove total, totalBD, and grandTotal from totals object
    const { total: _totalIgnore, totalBD: _totalBDIgnore, grandTotal: _grandTotalIgnore, ...cleanedTotals } = package.totals || {};
    const cleanedPackage = {
      ...package,
      totals: cleanedTotals
    };

    res.json({
      success: true,
      data: {
        ...cleanedPackage,
        assignedPassengerCounts: assignedCounts,
        totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
        totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
        profitOrLoss: Number(profitOrLoss.toFixed(2)),
        passengerOriginalTotals: {
          adult: Number(passengerOriginalTotals.adult.toFixed(2)),
          child: Number(passengerOriginalTotals.child.toFixed(2)),
          infant: Number(passengerOriginalTotals.infant.toFixed(2))
        },
        passengerCostingTotals: {
          adult: Number(passengerCostingTotals.adult.toFixed(2)),
          child: Number(passengerCostingTotals.child.toFixed(2)),
          infant: Number(passengerCostingTotals.infant.toFixed(2))
        },
        passengerProfit: {
          adult: Number(passengerProfit.adult.toFixed(2)),
          child: Number(passengerProfit.child.toFixed(2)),
          infant: Number(passengerProfit.infant.toFixed(2))
        },
        profitLoss: {
          assignedPassengerCounts: assignedCounts,
          originalPrices: originalPrices,
          costingPrices: costingPrices,
          totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
          totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
          profitOrLoss: Number(profitOrLoss.toFixed(2)),
          passengerOriginalTotals: {
            adult: Number(passengerOriginalTotals.adult.toFixed(2)),
            child: Number(passengerOriginalTotals.child.toFixed(2)),
            infant: Number(passengerOriginalTotals.infant.toFixed(2))
          },
          passengerCostingTotals: {
            adult: Number(passengerCostingTotals.adult.toFixed(2)),
            child: Number(passengerCostingTotals.child.toFixed(2)),
            infant: Number(passengerCostingTotals.infant.toFixed(2))
          },
          passengerProfit: {
            adult: Number(passengerProfit.adult.toFixed(2)),
            child: Number(passengerProfit.child.toFixed(2)),
            infant: Number(passengerProfit.infant.toFixed(2))
          }
        }
      }
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

    // Normalize costs.campFee if costs is being updated
    if (updateData.costs && typeof updateData.costs === 'object') {
      if (updateData.costs.campFee !== undefined) {
        updateData.costs.campFee = Number((parseFloat(updateData.costs.campFee) || 0).toFixed(2));
      }
    }

    // Preserve assignedPassengerCounts if not provided in update
    if (!updateData.assignedPassengerCounts) {
      const existingCounts = existingPackage.assignedPassengerCounts;
      if (existingCounts) {
        updateData.assignedPassengerCounts = existingCounts;
      } else {
        updateData.assignedPassengerCounts = {
          adult: 0,
          child: 0,
          infant: 0
        };
      }
    } else {
      // Ensure all three passenger types are present
      updateData.assignedPassengerCounts = {
        adult: parseInt(updateData.assignedPassengerCounts.adult) || 0,
        child: parseInt(updateData.assignedPassengerCounts.child) || 0,
        infant: parseInt(updateData.assignedPassengerCounts.infant) || 0
      };
    }

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
      // Preserve costingPassengerTotals if not provided
      if (!updateData.totals.costingPassengerTotals && existingPackage.totals?.costingPassengerTotals) {
        updateData.totals.costingPassengerTotals = existingPackage.totals.costingPassengerTotals;
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

// POST /haj-umrah/packages/:id/costing - Add/Update package costing
app.post('/haj-umrah/packages/:id/costing', async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid package ID'
      });
    }

    // Ensure package exists
    const existingPackage = await packages.findOne({ _id: new ObjectId(id) });
    if (!existingPackage) {
      return res.status(404).json({
        success: false,
        message: 'Package not found'
      });
    }

    const {
      sarToBdtRate: incomingSarToBdtRate,
      discount: incomingDiscount,
      costs: incomingCosts = {},
      bangladeshVisaPassengers,
      bangladeshAirfarePassengers,
      bangladeshBusPassengers,
      bangladeshTrainingOtherPassengers,
      saudiVisaPassengers,
      saudiMakkahHotelPassengers,
      saudiMadinaHotelPassengers,
      saudiMakkahFoodPassengers,
      saudiMadinaFoodPassengers,
      saudiMakkahZiyaraPassengers,
      saudiMadinaZiyaraPassengers,
      saudiTransportPassengers,
      saudiCampFeePassengers,
      saudiAlMashayerPassengers,
      saudiOthersPassengers,
      totalPrice: _totalPrice // Explicitly ignore totalPrice if sent in request
    } = req.body;

    const toNumber = (val, fallback = 0) => {
      const parsed = parseFloat(val);
      return Number.isFinite(parsed) ? parsed : fallback;
    };

    // Normalize air fare details (per passenger type)
    const normalizedAirFareDetails = {};
    if (incomingCosts.airFareDetails && typeof incomingCosts.airFareDetails === 'object') {
      ['adult', 'child', 'infant'].forEach((type) => {
        const price = incomingCosts.airFareDetails?.[type]?.price;
        normalizedAirFareDetails[type] = { price: toNumber(price, 0) };
      });
    }

    // Normalize hotel details (price & nights per passenger type)
    const normalizedHotelDetails = {};
    if (incomingCosts.hotelDetails && typeof incomingCosts.hotelDetails === 'object') {
      Object.keys(incomingCosts.hotelDetails).forEach((hotelKey) => {
        const hotel = incomingCosts.hotelDetails[hotelKey] || {};
        normalizedHotelDetails[hotelKey] = {};
        ['adult', 'child', 'infant'].forEach((ptype) => {
          const price = hotel?.[ptype]?.price;
          const nights = hotel?.[ptype]?.nights;
          normalizedHotelDetails[hotelKey][ptype] = {
            price: toNumber(price, 0),
            nights: toNumber(nights, 0)
          };
        });
      });
    }

    // Normalize other cost fields (default to 0)
    const normalizedCosts = {
      ...incomingCosts,
      airFareDetails: normalizedAirFareDetails,
      hotelDetails: normalizedHotelDetails
    };

    const numericCostFields = [
      'airFare',
      'makkahHotel1',
      'makkahHotel2',
      'makkahHotel3',
      'madinaHotel1',
      'madinaHotel2',
      'zamzamWater',
      'maktab',
      'visaFee',
      'insuranceFee',
      'electronicsFee',
      'groundServiceFee',
      'makkahRoute',
      'baggage',
      'serviceCharge',
      'monazzem',
      'food',
      'ziyaraFee',
      'idCard',
      'hajjKollan',
      'trainFee',
      'hajjGuide',
      'govtServiceCharge',
      'licenseFee',
      'transportFee',
      'otherBdCosts',
      'otherSaudiCosts',
      'train',
      'airFair',
      'campFee'
    ];

    numericCostFields.forEach((field) => {
      if (field in normalizedCosts) {
        normalizedCosts[field] = toNumber(normalizedCosts[field], 0);
      }
    });

    const sarToBdtRate = toNumber(incomingSarToBdtRate, 1);
    const discount = toNumber(incomingDiscount, 0);

    // Preserve existing passengerTotals (original prices) if they exist - define early to avoid reference errors
    const existingTotals = existingPackage.totals || {};
    const preservedPassengerTotals = existingTotals.passengerTotals || {
      adult: 0,
      child: 0,
      infant: 0
    };

    // Calculate totals similar to the frontend costCalc to keep server as source of truth
    // BD Costs (NOT multiplied by SAR rate)
    const bdCosts =
      toNumber(normalizedCosts.idCard) +
      toNumber(normalizedCosts.hajjKollan) +
      toNumber(normalizedCosts.trainFee) +
      toNumber(normalizedCosts.hajjGuide) +
      toNumber(normalizedCosts.govtServiceCharge) +
      toNumber(normalizedCosts.licenseFee) +
      toNumber(normalizedCosts.transportFee) +
      toNumber(normalizedCosts.otherBdCosts);

    // Saudi costs that need to be multiplied by SAR rate (including visaFee and insuranceFee)
    const saudiCostsSAR =
      toNumber(normalizedCosts.zamzamWater) +
      toNumber(normalizedCosts.maktab) +
      toNumber(normalizedCosts.electronicsFee) +
      toNumber(normalizedCosts.groundServiceFee) +
      toNumber(normalizedCosts.makkahRoute) +
      toNumber(normalizedCosts.baggage) +
      toNumber(normalizedCosts.serviceCharge) +
      toNumber(normalizedCosts.monazzem) +
      toNumber(normalizedCosts.food) +
      toNumber(normalizedCosts.ziyaraFee) +
      toNumber(normalizedCosts.campFee) +
      toNumber(normalizedCosts.visaFee) +
      toNumber(normalizedCosts.insuranceFee) +
      toNumber(normalizedCosts.otherSaudiCosts);
    
    const saudiCostsBDT = saudiCostsSAR * sarToBdtRate;

    const airFareBDT =
      toNumber(normalizedAirFareDetails?.adult?.price) +
      toNumber(normalizedAirFareDetails?.child?.price) +
      toNumber(normalizedAirFareDetails?.infant?.price);

    const hotelSar = Object.keys(normalizedHotelDetails).reduce((sum, key) => {
      const h = normalizedHotelDetails[key] || {};
      const adult = toNumber(h?.adult?.price) * toNumber(h?.adult?.nights);
      const child = toNumber(h?.child?.price) * toNumber(h?.child?.nights);
      const infant = toNumber(h?.infant?.price) * toNumber(h?.infant?.nights);
      return sum + adult + child + infant;
    }, 0);

    const saudiCostsBD = hotelSar * sarToBdtRate;
    const totalBD = bdCosts + saudiCostsBDT + airFareBDT + saudiCostsBD;
    const grandTotal = Math.max(0, totalBD - discount);

    // Calculate hotel costs - can come from hotelDetails (per passenger type) or direct fields (shared)
    const directHotelCosts = 
      toNumber(normalizedCosts.makkahHotel1) +
      toNumber(normalizedCosts.makkahHotel2) +
      toNumber(normalizedCosts.makkahHotel3) +
      toNumber(normalizedCosts.madinaHotel1) +
      toNumber(normalizedCosts.madinaHotel2);
    
    // Check if hotelDetails exist and have actual data
    const hasHotelDetails = Object.keys(normalizedHotelDetails).length > 0;
    let hasValidHotelDetails = false;
    if (hasHotelDetails) {
      // Check if any hotel has valid price and nights data
      for (const key of Object.keys(normalizedHotelDetails)) {
        const hotel = normalizedHotelDetails[key];
        if (hotel && (hotel.adult || hotel.child || hotel.infant)) {
          for (const type of ['adult', 'child', 'infant']) {
            if (hotel[type] && (toNumber(hotel[type].price) > 0 || toNumber(hotel[type].nights) > 0)) {
              hasValidHotelDetails = true;
              break;
            }
          }
          if (hasValidHotelDetails) break;
        }
      }
    }
    
    // passengerShared includes BD costs, Saudi costs, and direct hotel costs (if no valid hotelDetails)
    const passengerShared = bdCosts + saudiCostsBDT + (hasValidHotelDetails ? 0 : directHotelCosts);
    
    const passengerTotals = ['adult', 'child', 'infant'].reduce((acc, type) => {
      // Calculate hotel costs from hotelDetails (per passenger type) if valid, otherwise 0 (already in passengerShared)
      const totalHotelForType = hasValidHotelDetails 
        ? Object.keys(normalizedHotelDetails).reduce((sum, key) => {
            const h = normalizedHotelDetails[key] || {};
            const price = toNumber(h?.[type]?.price);
            const nights = toNumber(h?.[type]?.nights);
            return sum + price * nights * sarToBdtRate;
          }, 0)
        : 0;

      acc[type] =
        passengerShared +
        toNumber(normalizedAirFareDetails?.[type]?.price) +
        totalHotelForType;
      return acc;
    }, {});

    // Store costing prices separately - don't update original passengerTotals
    // costingPassengerTotals will be used for profit/loss calculation
    const costingPassengerTotals = {
      adult: Number((parseFloat(passengerTotals.adult) || 0).toFixed(2)),
      child: Number((parseFloat(passengerTotals.child) || 0).toFixed(2)),
      infant: Number((parseFloat(passengerTotals.infant) || 0).toFixed(2))
    };

    const computedTotals = {
      passengerTotals: preservedPassengerTotals, // Keep original prices
      costingPassengerTotals: costingPassengerTotals // Store costing prices separately
    };

    const updateData = {
      updatedAt: new Date(),
      sarToBdtRate,
      discount,
      costs: normalizedCosts,
      totals: computedTotals
    };

    // Update passenger arrays if provided
    if (bangladeshVisaPassengers !== undefined) {
      updateData.bangladeshVisaPassengers = bangladeshVisaPassengers || [];
    }
    if (bangladeshAirfarePassengers !== undefined) {
      updateData.bangladeshAirfarePassengers = bangladeshAirfarePassengers || [];
    }
    if (bangladeshBusPassengers !== undefined) {
      updateData.bangladeshBusPassengers = bangladeshBusPassengers || [];
    }
    if (bangladeshTrainingOtherPassengers !== undefined) {
      updateData.bangladeshTrainingOtherPassengers = bangladeshTrainingOtherPassengers || [];
    }
    if (saudiVisaPassengers !== undefined) {
      updateData.saudiVisaPassengers = saudiVisaPassengers || [];
    }
    if (saudiMakkahHotelPassengers !== undefined) {
      updateData.saudiMakkahHotelPassengers = saudiMakkahHotelPassengers || [];
    }
    if (saudiMadinaHotelPassengers !== undefined) {
      updateData.saudiMadinaHotelPassengers = saudiMadinaHotelPassengers || [];
    }
    if (saudiMakkahFoodPassengers !== undefined) {
      updateData.saudiMakkahFoodPassengers = saudiMakkahFoodPassengers || [];
    }
    if (saudiMadinaFoodPassengers !== undefined) {
      updateData.saudiMadinaFoodPassengers = saudiMadinaFoodPassengers || [];
    }
    if (saudiMakkahZiyaraPassengers !== undefined) {
      updateData.saudiMakkahZiyaraPassengers = saudiMakkahZiyaraPassengers || [];
    }
    if (saudiMadinaZiyaraPassengers !== undefined) {
      updateData.saudiMadinaZiyaraPassengers = saudiMadinaZiyaraPassengers || [];
    }
    if (saudiTransportPassengers !== undefined) {
      updateData.saudiTransportPassengers = saudiTransportPassengers || [];
    }
    if (saudiCampFeePassengers !== undefined) {
      updateData.saudiCampFeePassengers = saudiCampFeePassengers || [];
    }
    if (saudiAlMashayerPassengers !== undefined) {
      updateData.saudiAlMashayerPassengers = saudiAlMashayerPassengers || [];
    }
    if (saudiOthersPassengers !== undefined) {
      updateData.saudiOthersPassengers = saudiOthersPassengers || [];
    }

    // Important: totalPrice (main sale price) must NEVER be updated in this endpoint.
    // It should remain as the originally set sale price when package was created.
    // totals.grandTotal reflects the calculated costing, not the sale price.
    // We explicitly exclude totalPrice from updateData to ensure it's never changed.
    await packages.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    const updatedPackage = await packages.findOne({ _id: new ObjectId(id) });

    // Calculate Profit/Loss for the response (same logic as GET endpoint)
    const assignedCounts = updatedPackage.assignedPassengerCounts || {
      adult: 0,
      child: 0,
      infant: 0
    };

    // Get original prices (from totals.passengerTotals - set during package creation)
    const originalPrices = {
      adult: parseFloat(updatedPackage.totals?.passengerTotals?.adult) || 0,
      child: parseFloat(updatedPackage.totals?.passengerTotals?.child) || 0,
      infant: parseFloat(updatedPackage.totals?.passengerTotals?.infant) || 0
    };

    // Get costing prices (from totals.costingPassengerTotals - just calculated above)
    const costingPrices = {
      adult: parseFloat(updatedPackage.totals?.costingPassengerTotals?.adult) || 0,
      child: parseFloat(updatedPackage.totals?.costingPassengerTotals?.child) || 0,
      infant: parseFloat(updatedPackage.totals?.costingPassengerTotals?.infant) || 0
    };

    // Calculate Total Original Price based on assigned passengers
    const totalOriginalPrice = 
      (assignedCounts.adult * originalPrices.adult) +
      (assignedCounts.child * originalPrices.child) +
      (assignedCounts.infant * originalPrices.infant);

    // Calculate Total Costing Price based on assigned passengers
    const totalCostingPrice = 
      (assignedCounts.adult * costingPrices.adult) +
      (assignedCounts.child * costingPrices.child) +
      (assignedCounts.infant * costingPrices.infant);

    // Calculate Profit/Loss
    const profitOrLoss = totalOriginalPrice - totalCostingPrice;

    // Calculate per-type totals for display
    const passengerOriginalTotals = {
      adult: assignedCounts.adult * originalPrices.adult,
      child: assignedCounts.child * originalPrices.child,
      infant: assignedCounts.infant * originalPrices.infant
    };

    const passengerCostingTotals = {
      adult: assignedCounts.adult * costingPrices.adult,
      child: assignedCounts.child * costingPrices.child,
      infant: assignedCounts.infant * costingPrices.infant
    };

    const passengerProfit = {
      adult: passengerOriginalTotals.adult - passengerCostingTotals.adult,
      child: passengerOriginalTotals.child - passengerCostingTotals.child,
      infant: passengerOriginalTotals.infant - passengerCostingTotals.infant
    };

    // Remove total, totalBD, and grandTotal from totals object for response
    const { total: _total, totalBD: _totalBD, grandTotal: _grandTotal, ...cleanedTotals } = updatedPackage.totals || {};
    const cleanedPackage = {
      ...updatedPackage,
      totals: cleanedTotals
    };

    res.status(200).json({
      success: true,
      message: 'Package costing added/updated successfully',
      data: {
        ...cleanedPackage,
        assignedPassengerCounts: assignedCounts,
        totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
        totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
        profitOrLoss: Number(profitOrLoss.toFixed(2)),
        passengerOriginalTotals: {
          adult: Number(passengerOriginalTotals.adult.toFixed(2)),
          child: Number(passengerOriginalTotals.child.toFixed(2)),
          infant: Number(passengerOriginalTotals.infant.toFixed(2))
        },
        passengerCostingTotals: {
          adult: Number(passengerCostingTotals.adult.toFixed(2)),
          child: Number(passengerCostingTotals.child.toFixed(2)),
          infant: Number(passengerCostingTotals.infant.toFixed(2))
        },
        passengerProfit: {
          adult: Number(passengerProfit.adult.toFixed(2)),
          child: Number(passengerProfit.child.toFixed(2)),
          infant: Number(passengerProfit.infant.toFixed(2))
        },
        profitLoss: {
          assignedPassengerCounts: assignedCounts,
          originalPrices: originalPrices,
          costingPrices: costingPrices,
          totalOriginalPrice: Number(totalOriginalPrice.toFixed(2)),
          totalCostingPrice: Number(totalCostingPrice.toFixed(2)),
          profitOrLoss: Number(profitOrLoss.toFixed(2)),
          passengerOriginalTotals: {
            adult: Number(passengerOriginalTotals.adult.toFixed(2)),
            child: Number(passengerOriginalTotals.child.toFixed(2)),
            infant: Number(passengerOriginalTotals.infant.toFixed(2))
          },
          passengerCostingTotals: {
            adult: Number(passengerCostingTotals.adult.toFixed(2)),
            child: Number(passengerCostingTotals.child.toFixed(2)),
            infant: Number(passengerCostingTotals.infant.toFixed(2))
          },
          passengerProfit: {
            adult: Number(passengerProfit.adult.toFixed(2)),
            child: Number(passengerProfit.child.toFixed(2)),
            infant: Number(passengerProfit.infant.toFixed(2))
          }
        }
      }
    });
  } catch (error) {
    console.error('Add package costing error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to add/update package costing',
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

    // Increment assigned passenger count in package
    const currentCounts = package.assignedPassengerCounts || {
      adult: 0,
      child: 0,
      infant: 0
    };

    const updatedCounts = {
      ...currentCounts,
      [passengerTypeKey]: (currentCounts[passengerTypeKey] || 0) + 1
    };

    await packages.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          assignedPassengerCounts: updatedCounts,
          updatedAt: new Date()
        }
      }
    );

    const updatedPassenger = await targetCollection.findOne({ _id: passenger._id });
    const updatedPackage = await packages.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: `Package assigned successfully to ${passengerType}`,
      data: {
        passenger: updatedPassenger,
        package: {
          _id: updatedPackage._id,
          packageName: updatedPackage.packageName,
          passengerType: passengerTypeKey,
          price: selectedPrice,
          assignedPassengerCounts: updatedCounts
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

// ==================== HAJJ-UMRAH DASHBOARD SUMMARY ====================
// GET /haj-umrah/dashboard-summary - Get comprehensive dashboard statistics
app.get('/haj-umrah/dashboard-summary', async (req, res) => {
  try {
    // 1. Total Haji and Umrah counts
    const totalHajiCount = await haji.countDocuments({ isActive: { $ne: false } });
    const totalUmrahCount = await umrah.countDocuments({ isActive: { $ne: false } });
    
    // 2. Total Agents count
    const totalAgentsCount = await agents.countDocuments({ isActive: { $ne: false } });

    // 3. Profit/Loss for Hajj and Umrah from packages
    const allPackages = await packages.find({}).toArray();
    
    let hajjProfitLoss = {
      totalRevenue: 0,
      totalCost: 0,
      profitLoss: 0,
      packageCount: 0
    };
    
    let umrahProfitLoss = {
      totalRevenue: 0,
      totalCost: 0,
      profitLoss: 0,
      packageCount: 0
    };

    allPackages.forEach(pkg => {
      const packageType = (pkg.customPackageType || pkg.packageType || '').toLowerCase();
      const isHajj = packageType.includes('haj') || packageType.includes('hajj');
      const isUmrah = packageType.includes('umrah');
      
      const assignedCounts = pkg.assignedPassengerCounts || { adult: 0, child: 0, infant: 0 };
      const originalPrices = pkg.totals?.passengerTotals || { adult: 0, child: 0, infant: 0 };
      const costingPrices = pkg.totals?.costingPassengerTotals || { adult: 0, child: 0, infant: 0 };
      
      const revenue = 
        (assignedCounts.adult * (parseFloat(originalPrices.adult) || 0)) +
        (assignedCounts.child * (parseFloat(originalPrices.child) || 0)) +
        (assignedCounts.infant * (parseFloat(originalPrices.infant) || 0));
      
      const cost = 
        (assignedCounts.adult * (parseFloat(costingPrices.adult) || 0)) +
        (assignedCounts.child * (parseFloat(costingPrices.child) || 0)) +
        (assignedCounts.infant * (parseFloat(costingPrices.infant) || 0));
      
      if (isHajj) {
        hajjProfitLoss.totalRevenue += revenue;
        hajjProfitLoss.totalCost += cost;
        hajjProfitLoss.packageCount++;
      } else if (isUmrah) {
        umrahProfitLoss.totalRevenue += revenue;
        umrahProfitLoss.totalCost += cost;
        umrahProfitLoss.packageCount++;
      }
    });

    hajjProfitLoss.profitLoss = hajjProfitLoss.totalRevenue - hajjProfitLoss.totalCost;
    umrahProfitLoss.profitLoss = umrahProfitLoss.totalRevenue - umrahProfitLoss.totalCost;

    // Also calculate profit/loss from actual paid amounts (more accurate)
    // Get actual paid amounts from haji and umrah collections
    const hajiPaidStats = await haji.aggregate([
      { 
        $match: { 
          isActive: { $ne: false },
          $or: [
            { primaryHolderId: null },
            { $expr: { $eq: ['$primaryHolderId', '$_id'] } }
          ]
        } 
      },
      {
        $project: {
          familyTotal: { $ifNull: ['$familyTotal', 0] },
          familyPaid: { $ifNull: ['$familyPaid', 0] },
          totalAmount: { $ifNull: ['$totalAmount', 0] },
          paidAmount: { $ifNull: ['$paidAmount', 0] }
        }
      },
      {
        $project: {
          totalAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyTotal',
              '$totalAmount'
            ]
          },
          paidAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyPaid',
              '$paidAmount'
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          totalRevenue: { $sum: '$totalAmount' },
          totalPaid: { $sum: '$paidAmount' }
        }
      }
    ]).toArray();

    const umrahPaidStats = await umrah.aggregate([
      { 
        $match: { 
          isActive: { $ne: false },
          $or: [
            { primaryHolderId: null },
            { $expr: { $eq: ['$primaryHolderId', '$_id'] } }
          ]
        } 
      },
      {
        $project: {
          familyTotal: { $ifNull: ['$familyTotal', 0] },
          familyPaid: { $ifNull: ['$familyPaid', 0] },
          totalAmount: { $ifNull: ['$totalAmount', 0] },
          paidAmount: { $ifNull: ['$paidAmount', 0] }
        }
      },
      {
        $project: {
          totalAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyTotal',
              '$totalAmount'
            ]
          },
          paidAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyPaid',
              '$paidAmount'
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          totalRevenue: { $sum: '$totalAmount' },
          totalPaid: { $sum: '$paidAmount' }
        }
      }
    ]).toArray();

    // Use actual paid amounts for profit calculation (more accurate than package-based)
    const hajiActualRevenue = hajiPaidStats[0]?.totalRevenue || 0;
    const hajiActualPaid = hajiPaidStats[0]?.totalPaid || 0;
    const umrahActualRevenue = umrahPaidStats[0]?.totalRevenue || 0;
    const umrahActualPaid = umrahPaidStats[0]?.totalPaid || 0;

    // Update profitLoss with actual data (use package cost if available, otherwise use 0)
    // Profit = Revenue (paid amount) - Cost (from packages)
    hajjProfitLoss.totalRevenue = hajiActualRevenue;
    hajjProfitLoss.profitLoss = hajiActualPaid - hajjProfitLoss.totalCost;
    
    umrahProfitLoss.totalRevenue = umrahActualRevenue;
    umrahProfitLoss.profitLoss = umrahActualPaid - umrahProfitLoss.totalCost;

    // 4. Agent-wise Profit/Loss (only for active agents) - UPDATED to use calculateFinancialSummary
    // First, get all active agent IDs
    const activeAgents = await agents.find({ isActive: { $ne: false } }).toArray();
    const activeAgentIds = new Set(activeAgents.map(a => String(a._id)));

    // Calculate profit/loss for each agent using the same logic as GET agent endpoint
    const agentProfitLossArray = await Promise.all(
      activeAgents.map(async (agent) => {
        const agentId = String(agent._id);
        
        // Fetch all packages for this agent
        const packagesForAgent = await agentPackages
          .find({ agentId: new ObjectId(agentId) })
          .toArray();
        
        // Calculate financial summary using the same helper function
        const financialSummary = calculateFinancialSummary(packagesForAgent);
        
        return {
          agentId: agentId,
          agentName: agent.tradeName || agent.ownerName || 'Unknown',
          totalRevenue: Number(financialSummary.overall.billed.toFixed(2)),
          totalCost: Number(financialSummary.overall.costingPrice.toFixed(2)),
          profitLoss: Number(financialSummary.overall.profit.toFixed(2)),
          totalPaid: Number(financialSummary.overall.paid.toFixed(2)),
          totalDue: Number(financialSummary.overall.due.toFixed(2)),
          totalAdvance: Number(financialSummary.overall.advance.toFixed(2)),
          packageCount: packagesForAgent.length,
          customerCount: financialSummary.overall.customers
        };
      })
    );

    // Sort by profit/loss (highest first)
    const filteredAgentProfitLoss = agentProfitLossArray.sort((a, b) => b.profitLoss - a.profitLoss);

    // 5. Agent with most Haji
    // Get primary holders (where primaryHolderId is null or equals their own _id)
    // Check both packageInfo.agentId from haji and agentPackages
    const hajiByAgentFromPackageInfo = await haji.aggregate([
      { 
        $match: { 
          isActive: { $ne: false },
          $or: [
            { primaryHolderId: null },
            { $expr: { $eq: ['$primaryHolderId', '$_id'] } }
          ],
          'packageInfo.agentId': { $exists: true, $ne: null }
        } 
      },
      {
        $lookup: {
          from: 'agents',
          let: { agentId: '$packageInfo.agentId' },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ['$_id', '$$agentId'] },
                isActive: { $ne: false }
              }
            }
          ],
          as: 'agentDetails'
        }
      },
      {
        $unwind: {
          path: '$agentDetails',
          preserveNullAndEmptyArrays: false // Only include if agent exists and is active
        }
      },
      {
        $group: {
          _id: '$agentDetails._id',
          agentName: { $first: '$agentDetails.tradeName' },
          ownerName: { $first: '$agentDetails.ownerName' },
          hajiCount: { $sum: 1 }
        }
      }
    ]).toArray();

    // Also check from agentPackages assignedCustomers
    const agentPackagesWithCustomers = await agentPackages.aggregate([
      {
        $match: { assignedCustomers: { $exists: true, $ne: [] } }
      },
      {
        $project: {
          agentId: 1,
          assignedCustomers: 1
        }
      },
      {
        $lookup: {
          from: 'agents',
          let: { agentId: '$agentId' },
          pipeline: [
            {
              $match: {
                $expr: { $eq: ['$_id', '$$agentId'] },
                isActive: { $ne: false }
              }
            }
          ],
          as: 'agentInfo'
        }
      },
      {
        $unwind: {
          path: '$agentInfo',
          preserveNullAndEmptyArrays: false // Only include if agent exists and is active
        }
      },
      {
        $group: {
          _id: '$agentId',
          agentName: { $first: '$agentInfo.tradeName' },
          ownerName: { $first: '$agentInfo.ownerName' },
          totalCustomers: { $sum: { $size: '$assignedCustomers' } }
        }
      },
      { $sort: { totalCustomers: -1 } },
      { $limit: 10 }
    ]).toArray();

    // Merge and combine results (only for active agents)
    const agentHajiMap = new Map();
    hajiByAgentFromPackageInfo.forEach(item => {
      if (!item._id) return;
      const agentId = String(item._id);
      // Double-check agent is active
      if (!activeAgentIds.has(agentId)) return;
      
      if (!agentHajiMap.has(agentId)) {
        agentHajiMap.set(agentId, {
          agentId: agentId,
          agentName: item.agentName || item.ownerName || 'Unknown',
          hajiCount: 0
        });
      }
      agentHajiMap.get(agentId).hajiCount += item.hajiCount || 0;
    });

    agentPackagesWithCustomers.forEach(item => {
      if (!item._id) return;
      const agentId = String(item._id);
      // Double-check agent is active
      if (!activeAgentIds.has(agentId)) return;
      
      if (!agentHajiMap.has(agentId)) {
        agentHajiMap.set(agentId, {
          agentId: agentId,
          agentName: item.agentName || item.ownerName || 'Unknown',
          hajiCount: 0
        });
      }
      agentHajiMap.get(agentId).hajiCount += (item.totalCustomers || 0);
    });

    const topAgentsByHaji = Array.from(agentHajiMap.values())
      .sort((a, b) => b.hajiCount - a.hajiCount)
      .slice(0, 10);

    // 6. District-wise Haji count
    const hajiByDistrict = await haji.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: '$district',
          hajiCount: { $sum: 1 },
          umrahCount: { $sum: { $cond: [{ $eq: ['$serviceType', 'umrah'] }, 1, 0] } }
        }
      },
      { $sort: { hajiCount: -1 } },
      { $limit: 20 }
    ]).toArray();

    // Also get Umrah by district
    const umrahByDistrict = await umrah.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: '$district',
          umrahCount: { $sum: 1 }
        }
      },
      { $sort: { umrahCount: -1 } },
      { $limit: 20 }
    ]).toArray();

    // Merge district data
    const districtMap = new Map();
    hajiByDistrict.forEach(item => {
      const district = item._id || 'Unknown';
      districtMap.set(district, {
        district: district,
        hajiCount: item.hajiCount || 0,
        umrahCount: item.umrahCount || 0
      });
    });

    umrahByDistrict.forEach(item => {
      const district = item._id || 'Unknown';
      if (!districtMap.has(district)) {
        districtMap.set(district, {
          district: district,
          hajiCount: 0,
          umrahCount: 0
        });
      }
      districtMap.get(district).umrahCount += (item.umrahCount || 0);
    });

    const topDistricts = Array.from(districtMap.values())
      .map(d => ({
        ...d,
        totalCount: d.hajiCount + d.umrahCount
      }))
      .sort((a, b) => b.totalCount - a.totalCount)
      .slice(0, 20);

    // 7. Additional statistics
    // Total revenue and payments
    const hajiTransactions = await transactions.aggregate([
      {
        $match: {
          partyType: 'haji',
          isActive: { $ne: false },
          transactionType: 'credit'
        }
      },
      {
        $group: {
          _id: null,
          totalPaid: { $sum: '$amount' }
        }
      }
    ]).toArray();

    const umrahTransactions = await transactions.aggregate([
      {
        $match: {
          partyType: 'umrah',
          isActive: { $ne: false },
          transactionType: 'credit'
        }
      },
      {
        $group: {
          _id: null,
          totalPaid: { $sum: '$amount' }
        }
      }
    ]).toArray();

    const totalHajiPaid = hajiTransactions[0]?.totalPaid || 0;
    const totalUmrahPaid = umrahTransactions[0]?.totalPaid || 0;

    // Total due amounts (only primary holders)
    // Use familyTotal/familyPaid if available, otherwise fallback to totalAmount/paidAmount
    const hajiTotalDue = await haji.aggregate([
      { 
        $match: { 
          isActive: { $ne: false },
          $or: [
            { primaryHolderId: null },
            { $expr: { $eq: ['$primaryHolderId', '$_id'] } }
          ]
        } 
      },
      {
        $project: {
          familyTotal: { $ifNull: ['$familyTotal', 0] },
          familyPaid: { $ifNull: ['$familyPaid', 0] },
          totalAmount: { $ifNull: ['$totalAmount', 0] },
          paidAmount: { $ifNull: ['$paidAmount', 0] }
        }
      },
      {
        $project: {
          totalAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyTotal',
              '$totalAmount'
            ]
          },
          totalPaid: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyPaid',
              '$paidAmount'
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: '$totalAmount' },
          totalPaid: { $sum: '$totalPaid' },
          totalDue: { $sum: { $subtract: ['$totalAmount', '$totalPaid'] } }
        }
      }
    ]).toArray();

    const umrahTotalDue = await umrah.aggregate([
      { 
        $match: { 
          isActive: { $ne: false },
          $or: [
            { primaryHolderId: null },
            { $expr: { $eq: ['$primaryHolderId', '$_id'] } }
          ]
        } 
      },
      {
        $project: {
          familyTotal: { $ifNull: ['$familyTotal', 0] },
          familyPaid: { $ifNull: ['$familyPaid', 0] },
          totalAmount: { $ifNull: ['$totalAmount', 0] },
          paidAmount: { $ifNull: ['$paidAmount', 0] }
        }
      },
      {
        $project: {
          totalAmount: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyTotal',
              '$totalAmount'
            ]
          },
          totalPaid: {
            $cond: [
              { $gt: ['$familyTotal', 0] },
              '$familyPaid',
              '$paidAmount'
            ]
          }
        }
      },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: '$totalAmount' },
          totalPaid: { $sum: '$totalPaid' },
          totalDue: { $sum: { $subtract: ['$totalAmount', '$totalPaid'] } }
        }
      }
    ]).toArray();

    // Agent total due (only active agents)
    const agentTotalDue = await agents.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalDue: { $sum: '$totalDue' },
          hajDue: { $sum: '$hajDue' },
          umrahDue: { $sum: '$umrahDue' }
        }
      }
    ]).toArray();

    res.json({
      success: true,
      data: {
        overview: {
          totalHaji: totalHajiCount,
          totalUmrah: totalUmrahCount,
          totalAgents: totalAgentsCount,
          totalPilgrims: totalHajiCount + totalUmrahCount
        },
        profitLoss: {
          hajj: {
            totalRevenue: Number(hajjProfitLoss.totalRevenue.toFixed(2)),
            totalCost: Number(hajjProfitLoss.totalCost.toFixed(2)),
            profitLoss: Number(hajjProfitLoss.profitLoss.toFixed(2)),
            packageCount: hajjProfitLoss.packageCount,
            isProfit: hajjProfitLoss.profitLoss > 0
          },
          umrah: {
            totalRevenue: Number(umrahProfitLoss.totalRevenue.toFixed(2)),
            totalCost: Number(umrahProfitLoss.totalCost.toFixed(2)),
            profitLoss: Number(umrahProfitLoss.profitLoss.toFixed(2)),
            packageCount: umrahProfitLoss.packageCount,
            isProfit: umrahProfitLoss.profitLoss > 0
          },
          combined: {
            totalRevenue: Number((hajjProfitLoss.totalRevenue + umrahProfitLoss.totalRevenue).toFixed(2)),
            totalCost: Number((hajjProfitLoss.totalCost + umrahProfitLoss.totalCost).toFixed(2)),
            profitLoss: Number((hajjProfitLoss.profitLoss + umrahProfitLoss.profitLoss).toFixed(2)),
            isProfit: (hajjProfitLoss.profitLoss + umrahProfitLoss.profitLoss) > 0
          }
        },
        agentProfitLoss: filteredAgentProfitLoss,
        topAgentsByHaji: topAgentsByHaji,
        topDistricts: topDistricts,
        financialSummary: {
          totalDue: Number((
            (hajiTotalDue[0]?.totalDue || 0) +
            (umrahTotalDue[0]?.totalDue || 0) +
            (agentTotalDue[0]?.totalDue || 0)
          ).toFixed(2)),
          haji: {
            totalAmount: Number((hajiTotalDue[0]?.totalAmount || 0).toFixed(2)),
            totalPaid: Number((hajiTotalDue[0]?.totalPaid || 0).toFixed(2)),
            totalDue: Number((hajiTotalDue[0]?.totalDue || 0).toFixed(2))
          },
          umrah: {
            totalAmount: Number((umrahTotalDue[0]?.totalAmount || 0).toFixed(2)),
            totalPaid: Number((umrahTotalDue[0]?.totalPaid || 0).toFixed(2)),
            totalDue: Number((umrahTotalDue[0]?.totalDue || 0).toFixed(2))
          },
          agents: {
            totalDue: Number((agentTotalDue[0]?.totalDue || 0).toFixed(2)),
            hajDue: Number((agentTotalDue[0]?.hajDue || 0).toFixed(2)),
            umrahDue: Number((agentTotalDue[0]?.umrahDue || 0).toFixed(2))
          }
        }
      }
    });
  } catch (error) {
    console.error('Dashboard summary error:', error);
    res.status(500).json({
      success: false,
      error: true,
      message: 'Failed to fetch dashboard summary',
      details: error.message
    });
  }
});

// ==================== BANK ACCOUNTS ROUTES ====================
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
    const { fromDate, toDate, page = 1, limit = 20, type } = req.query || {};

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid bank account ID'
      });
    }

    // Validate bank account exists
    const account = await bankAccounts.findOne({ _id: new ObjectId(id), isDeleted: { $ne: true } });
    if (!account) {
      return res.status(404).json({
        success: false,
        message: 'Bank account not found'
      });
    }

    const accountIdStr = String(id);
    const accountIdObj = new ObjectId(id);
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 20, 1), 100);

    // Build comprehensive filter for bank account transactions
    const filter = {
      isActive: { $ne: false },
      $or: [
        // Direct bankAccountId match
        { bankAccountId: accountIdObj },
        // targetAccountId match (for credit/debit transactions)
        { targetAccountId: accountIdStr },
        { targetAccountId: accountIdObj },
        // fromAccountId match (for transfers out)
        { fromAccountId: accountIdStr },
        { fromAccountId: accountIdObj },
        // toAccountId match (for transfers in)
        { toAccountId: accountIdStr },
        { toAccountId: accountIdObj },
        // paymentDetails match
        {
          "paymentDetails.bankName": account.bankName,
          "paymentDetails.accountNumber": account.accountNumber
        },
        // transferDetails match
        { "transferDetails.fromAccountId": accountIdStr },
        { "transferDetails.fromAccountId": accountIdObj },
        { "transferDetails.toAccountId": accountIdStr },
        { "transferDetails.toAccountId": accountIdObj }
      ]
    };

    // Add transaction type filter
    if (type && ['credit', 'debit', 'transfer'].includes(type)) {
      if (type === 'transfer') {
        filter.isTransfer = true;
      } else {
        filter.transactionType = type;
        filter.isTransfer = { $ne: true };
      }
    }

    // Add date filter
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

    // Build cursor for transactions
    const cursor = transactions
      .find(filter)
      .sort({ date: -1, createdAt: -1 })
      .skip((pageNum - 1) * limitNum)
      .limit(limitNum);

    // Build aggregation pipeline for totals
    const totalsPipeline = [
      { $match: filter },
      {
        $group: {
          _id: null,
          totalCredit: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $eq: ['$transactionType', 'credit'] },
                    { $ne: ['$isTransfer', true] }
                  ]
                },
                { $ifNull: ['$amount', { $ifNull: ['$paymentDetails.amount', 0] }] },
                0
              ]
            }
          },
          totalDebit: {
            $sum: {
              $cond: [
                {
                  $and: [
                    { $eq: ['$transactionType', 'debit'] },
                    { $ne: ['$isTransfer', true] }
                  ]
                },
                { $ifNull: ['$amount', { $ifNull: ['$paymentDetails.amount', 0] }] },
                0
              ]
            }
          },
          lastTransactionDate: {
            $max: '$date'
          }
        }
      }
    ];

    // Build filter for transfer transactions only
    const transferFilter = {
      ...filter,
      isTransfer: true
    };

    // Execute queries in parallel
    const [items, total, totalsAgg, transferTransactions] = await Promise.all([
      cursor.toArray(),
      transactions.countDocuments(filter),
      transactions.aggregate(totalsPipeline).toArray(),
      transactions.find(transferFilter).toArray()
    ]);

    const totals = totalsAgg?.[0] || {};
    const totalCredit = totals.totalCredit || 0;
    const totalDebit = totals.totalDebit || 0;
    
    // Calculate transfer amounts from transfer transactions
    let totalTransferIn = 0;
    let totalTransferOut = 0;
    
    transferTransactions.forEach(tx => {
      const transferAmount = tx.amount || tx.transferDetails?.transferAmount || 0;
      const toAccountId = tx.toAccountId?.toString() || tx.transferDetails?.toAccountId?.toString();
      const fromAccountId = tx.fromAccountId?.toString() || tx.transferDetails?.fromAccountId?.toString();
      
      if (toAccountId === accountIdStr || (toAccountId && ObjectId.isValid(toAccountId) && new ObjectId(toAccountId).equals(accountIdObj))) {
        totalTransferIn += transferAmount;
      }
      if (fromAccountId === accountIdStr || (fromAccountId && ObjectId.isValid(fromAccountId) && new ObjectId(fromAccountId).equals(accountIdObj))) {
        totalTransferOut += transferAmount;
      }
    });
    
    const net = totalCredit + totalTransferIn - totalDebit - totalTransferOut;

    res.json({
      success: true,
      data: items,
      account: {
        id: String(account._id),
        bankName: account.bankName,
        accountNumber: account.accountNumber,
        accountHolder: account.accountHolder,
        accountTitle: account.accountTitle,
        currentBalance: account.currentBalance,
        initialBalance: account.initialBalance,
        currency: account.currency,
        accountCategory: account.accountCategory,
        status: account.status
      },
      totals: {
        totalCredit,
        totalDebit,
        totalTransferIn,
        totalTransferOut,
        net,
        lastTransactionDate: totals.lastTransactionDate || null
      },
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        totalPages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Get bank account transactions error:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to fetch transaction history',
      error: error.message
    });
  }
});

// Get bank account transaction summary/statistics
app.get("/bank-accounts/:id/transactions/summary", async (req, res) => {
  try {
    const { id } = req.params;
    const { startDate, endDate } = req.query;

    // Validate ObjectId
    if (!ObjectId.isValid(id)) {
      return res.status(400).json({ success: false, error: "Invalid bank account ID format" });
    }

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
        },
        { "transferDetails.fromAccountId": new ObjectId(id), isTransfer: true },
        { "transferDetails.toAccountId": new ObjectId(id), isTransfer: true }
      ],
      isActive: { $ne: false }
    };

    if (startDate || endDate) {
      query.date = {};
      if (startDate) query.date.$gte = new Date(startDate);
      if (endDate) query.date.$lte = new Date(endDate);
    }

    // Get all transactions for summary
    const allTransactions = await transactions.find(query).toArray();

    // Calculate statistics
    let totalCredit = 0;
    let totalDebit = 0;
    let totalTransferIn = 0;
    let totalTransferOut = 0;
    let creditCount = 0;
    let debitCount = 0;
    let transferInCount = 0;
    let transferOutCount = 0;

    allTransactions.forEach(tx => {
      if (tx.isTransfer) {
        if (tx.transferDetails?.toAccountId?.toString() === id) {
          totalTransferIn += tx.transferDetails?.transferAmount || 0;
          transferInCount++;
        }
        if (tx.transferDetails?.fromAccountId?.toString() === id) {
          totalTransferOut += tx.transferDetails?.transferAmount || 0;
          transferOutCount++;
        }
      } else {
        const amount = tx.paymentDetails?.amount || tx.amount || 0;
        if (tx.transactionType === 'credit') {
          totalCredit += amount;
          creditCount++;
        } else if (tx.transactionType === 'debit') {
          totalDebit += amount;
          debitCount++;
        }
      }
    });

    const netAmount = totalCredit + totalTransferIn - totalDebit - totalTransferOut;

    res.json({
      success: true,
      data: {
        account: {
          _id: account._id,
          bankName: account.bankName,
          accountNumber: account.accountNumber,
          currentBalance: account.currentBalance,
          currency: account.currency
        },
        period: {
          startDate: startDate || null,
          endDate: endDate || null
        },
        statistics: {
          totalTransactions: allTransactions.length,
          credit: {
            count: creditCount,
            total: totalCredit
          },
          debit: {
            count: debitCount,
            total: totalDebit
          },
          transferIn: {
            count: transferInCount,
            total: totalTransferIn
          },
          transferOut: {
            count: transferOutCount,
            total: totalTransferOut
          },
          netAmount: netAmount,
          netChange: netAmount
        }
      }
    });
  } catch (error) {
    console.error("❌ Error getting bank account transaction summary:", error);
    res.status(500).json({ success: false, error: "Failed to get transaction summary" });
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
            await triggerFamilyRecomputeForHaji(after);

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
            await triggerFamilyRecomputeForUmrah(after);

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
app.post("/api/hr/employers", async (req, res) => {
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
app.get("/api/hr/employers", async (req, res) => {
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
app.get("/api/hr/employers/:id", async (req, res) => {
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
app.put("/api/hr/employers/:id", async (req, res) => {
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
app.delete("/api/hr/employers/:id", async (req, res) => {
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
app.get("/api/hr/employers/stats/overview", async (req, res) => {
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

// ==================== FARM EMPLOYEE MANAGEMENT (Simple API) ====================

// Helper to generate simple sequential IDs with prefix (stored in counters)
async function generateSequentialId(prefix) {
  const key = `seq_${prefix}`;
  const result = await counters.findOneAndUpdate(
    { counterKey: key },
    { $inc: { sequence: 1 } },
    { upsert: true, returnDocument: 'after' }
  );
  const seq = String((result.value?.sequence) || 1).padStart(3, '0');
  return `${prefix}${seq}`;
}

// POST: Create employee
app.post("/api/employees", async (req, res) => {
  try {
    const {
      name,
      position,
      phone,
      email = '',
      address = '',
      joinDate,
      salary,
      workHours,
      status = 'active',
      notes = ''
    } = req.body || {};

    if (!name || !position || !phone || !joinDate || salary === undefined || workHours === undefined) {
      return res.status(400).json({ success: false, message: "Missing required fields" });
    }

    const id = await generateSequentialId('EMP');

    const doc = {
      id,
      name: String(name).trim(),
      position: String(position).trim(),
      phone: String(phone).trim(),
      email: String(email || '').trim(),
      address: String(address || '').trim(),
      joinDate: String(joinDate), // yyyy-mm-dd
      salary: Number(salary),
      workHours: Number(workHours),
      status: String(status),
      notes: String(notes || ''),
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await farmEmployees.insertOne(doc);
    res.json({ success: true, data: doc });
  } catch (e) {
    console.error('Create farm employee error:', e);
    res.status(500).json({ success: false, message: 'Failed to create employee' });
  }
});

// GET: List employees with optional filters: search, status
app.get("/api/employees", async (req, res) => {
  try {
    const { search = '', status = 'all' } = req.query || {};
    const filter = { isActive: { $ne: false } };
    if (status && status !== 'all') filter.status = String(status);
    if (search) {
      const s = String(search);
      filter.$or = [
        { name: { $regex: s, $options: 'i' } },
        { position: { $regex: s, $options: 'i' } },
        { phone: { $regex: s } }
      ];
    }
    const data = await farmEmployees.find(filter).sort({ createdAt: -1 }).toArray();
    res.json({ success: true, data });
  } catch (e) {
    console.error('List farm employees error:', e);
    res.status(500).json({ success: false, message: 'Failed to fetch employees' });
  }
});

// GET: Single employee by id
app.get("/api/employees/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const emp = await farmEmployees.findOne({ id, isActive: { $ne: false } });
    if (!emp) return res.status(404).json({ success: false, message: 'Employee not found' });
    res.json({ success: true, data: emp });
  } catch (e) {
    console.error('Get farm employee error:', e);
    res.status(500).json({ success: false, message: 'Failed to fetch employee' });
  }
});

// DELETE: Employee by id (soft delete) and cascade attendance soft delete
app.delete("/api/employees/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const emp = await farmEmployees.findOne({ id, isActive: { $ne: false } });
    if (!emp) return res.status(404).json({ success: false, message: 'Employee not found' });
    await farmEmployees.updateOne({ id }, { $set: { isActive: false, updatedAt: new Date() } });
    await attendanceRecords.updateMany({ employeeId: id }, { $set: { isActive: false, updatedAt: new Date() } });
    res.json({ success: true, message: 'Employee deleted' });
  } catch (e) {
    console.error('Delete farm employee error:', e);
    res.status(500).json({ success: false, message: 'Failed to delete employee' });
  }
});

// POST: Attendance
app.post("/api/attendance", async (req, res) => {
  try {
    const {
      employeeId,
      date,
      checkIn = '',
      checkOut = '',
      status = 'present',
      notes = ''
    } = req.body || {};

    if (!employeeId || !date) {
      return res.status(400).json({ success: false, message: 'employeeId and date are required' });
    }

    const emp = await farmEmployees.findOne({ id: employeeId, isActive: { $ne: false } });
    if (!emp) return res.status(404).json({ success: false, message: 'Employee not found' });

    const attId = await generateSequentialId('ATT');
    const doc = {
      id: attId,
      employeeId,
      employeeName: emp.name,
      date: String(date),
      checkIn: String(checkIn || ''),
      checkOut: String(checkOut || ''),
      status: String(status),
      notes: String(notes || ''),
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await attendanceRecords.insertOne(doc);
    res.json({ success: true, data: doc });
  } catch (e) {
    console.error('Create attendance error:', e);
    res.status(500).json({ success: false, message: 'Failed to add attendance' });
  }
});

// GET: Attendance (optional: limit)
app.get("/api/attendance", async (req, res) => {
  try {
    const { limit = 20, employeeId, date } = req.query || {};
    const filter = { isActive: { $ne: false } };
    if (employeeId) filter.employeeId = String(employeeId);
    if (date) filter.date = String(date);
    const data = await attendanceRecords
      .find(filter)
      .sort({ date: -1, createdAt: -1 })
      .limit(parseInt(limit))
      .toArray();
    res.json({ success: true, data });
  } catch (e) {
    console.error('List attendance error:', e);
    res.status(500).json({ success: false, message: 'Failed to fetch attendance' });
  }
});

// GET: Employee stats for dashboard
app.get("/api/employees/stats", async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const thisMonth = new Date().toISOString().slice(0, 7);

    const allEmployees = await farmEmployees.find({ isActive: { $ne: false } }).toArray();
    const activeEmployees = allEmployees.filter(e => e.status === 'active').length;
    const totalSalary = allEmployees
      .filter(e => e.status === 'active')
      .reduce((sum, e) => sum + (Number(e.salary) || 0), 0);

    const todays = await attendanceRecords.find({ date: today, isActive: { $ne: false } }).toArray();
    const presentToday = todays.filter(a => a.status === 'present').length;
    const absentToday = todays.filter(a => a.status === 'absent').length;

    const monthlyAttendance = await attendanceRecords.countDocuments({
      date: { $regex: `^${thisMonth}` },
      status: 'present',
      isActive: { $ne: false }
    });

    res.json({
      success: true,
      data: {
        totalEmployees: allEmployees.length,
        activeEmployees,
        totalSalary,
        monthlyAttendance,
        presentToday,
        absentToday
      }
    });
  } catch (e) {
    console.error('Get employee stats error:', e);
    res.status(500).json({ success: false, message: 'Failed to fetch stats' });
  }
});





// ==================== FARM FINANCIAL RECORDS (Expenses & Incomes) ====================

// Helpers to normalize output
const normalizeFarmExpense = (doc) => ({
  id: doc.id,
  _id: String(doc._id || ''),
  category: doc.category,
  description: doc.description,
  amount: Number(doc.amount) || 0,
  vendor: String(doc.vendor || ''),
  notes: String(doc.notes || ''),
  createdAt: doc.createdAt,
  updatedAt: doc.updatedAt
});

const normalizeFarmIncome = (doc) => ({
  id: doc.id,
  _id: String(doc._id || ''),
  source: doc.source,
  description: doc.description,
  amount: Number(doc.amount) || 0,
  date: String(doc.date || ''),
  paymentMethod: String(doc.paymentMethod || 'cash'),
  customer: String(doc.customer || ''),
  notes: String(doc.notes || ''),
  createdAt: doc.createdAt,
  updatedAt: doc.updatedAt
});

// ---------- Expenses ----------

// CREATE expense
app.post("/api/farm/expenses", async (req, res) => {
  try {
    const {
      category = '',
      description = '',
      amount = 0,
      vendor = '',
      notes = ''
    } = req.body || {};

    if (!category || !description) {
      return res.status(400).json({ success: false, message: "category and description are required" });
    }

    const nowId = Date.now();
    const doc = {
      id: nowId,
      category: String(category),
      description: String(description),
      amount: Number(amount) || 0,
      vendor: String(vendor || ''),
      notes: String(notes || ''),
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await farmExpenses.insertOne(doc);
    return res.status(201).json({ success: true, data: normalizeFarmExpense(doc) });
  } catch (e) {
    console.error('Create farm expense error:', e);
    return res.status(500).json({ success: false, message: 'Failed to create expense' });
  }
});

// LIST expenses (filters: search, date)
app.get("/api/farm/expenses", async (req, res) => {
  try {
    const { search = '' } = req.query || {};
    const filter = {};
    if (search) {
      const s = String(search);
      filter.$or = [
        { vendor: { $regex: s, $options: 'i' } },
        { description: { $regex: s, $options: 'i' } },
        { notes: { $regex: s, $options: 'i' } }
      ];
    }
    const list = await farmExpenses.find(filter).sort({ createdAt: -1 }).toArray();
    return res.json({ success: true, data: list.map(normalizeFarmExpense) });
  } catch (e) {
    console.error('List farm expenses error:', e);
    return res.status(500).json({ success: false, message: 'Failed to fetch expenses' });
  }
});

// GET one expense by id
app.get("/api/farm/expenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const doc = await farmExpenses.findOne({ id: numericId });
    if (!doc) return res.status(404).json({ success: false, message: 'Expense not found' });
    return res.json({ success: true, data: normalizeFarmExpense(doc) });
  } catch (e) {
    console.error('Get farm expense error:', e);
    return res.status(500).json({ success: false, message: 'Failed to fetch expense' });
  }
});

// UPDATE expense by id
app.put("/api/farm/expenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const updateFields = {};
    const allowed = ['category', 'description', 'vendor', 'notes', 'amount'];
    for (const key of allowed) {
      if (req.body && Object.prototype.hasOwnProperty.call(req.body, key)) {
        updateFields[key] = key === 'amount' ? Number(req.body[key]) || 0 : String(req.body[key] ?? '');
      }
    }
    updateFields.updatedAt = new Date();

    const result = await farmExpenses.findOneAndUpdate(
      { id: numericId },
      { $set: updateFields },
      { returnDocument: 'after' }
    );
    if (!result.value) return res.status(404).json({ success: false, message: 'Expense not found' });
    return res.json({ success: true, data: normalizeFarmExpense(result.value) });
  } catch (e) {
    console.error('Update farm expense error:', e);
    return res.status(500).json({ success: false, message: 'Failed to update expense' });
  }
});

// DELETE expense by id
app.delete("/api/farm/expenses/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const del = await farmExpenses.deleteOne({ id: numericId });
    if (del.deletedCount === 0) return res.status(404).json({ success: false, message: 'Expense not found' });
    return res.json({ success: true });
  } catch (e) {
    console.error('Delete farm expense error:', e);
    return res.status(500).json({ success: false, message: 'Failed to delete expense' });
  }
});

// ---------- Incomes ----------

// CREATE income
app.post("/api/farm/incomes", async (req, res) => {
  try {
    const {
      source = '',
      description = '',
      amount = 0,
      date = new Date().toISOString().split('T')[0],
      paymentMethod = 'cash',
      customer = '',
      notes = ''
    } = req.body || {};

    if (!source || !description) {
      return res.status(400).json({ success: false, message: "source and description are required" });
    }

    const nowId = Date.now();
    const doc = {
      id: nowId,
      source: String(source),
      description: String(description),
      amount: Number(amount) || 0,
      date: String(date), // yyyy-mm-dd
      paymentMethod: String(paymentMethod || 'cash'),
      customer: String(customer || ''),
      notes: String(notes || ''),
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await farmIncomes.insertOne(doc);
    return res.status(201).json({ success: true, data: normalizeFarmIncome(doc) });
  } catch (e) {
    console.error('Create farm income error:', e);
    return res.status(500).json({ success: false, message: 'Failed to create income' });
  }
});

// LIST incomes (filters: search, date)
app.get("/api/farm/incomes", async (req, res) => {
  try {
    const { search = '', date = '' } = req.query || {};
    const filter = {};
    if (date) filter.date = String(date);
    if (search) {
      const s = String(search);
      filter.$or = [
        { customer: { $regex: s, $options: 'i' } },
        { description: { $regex: s, $options: 'i' } },
        { notes: { $regex: s, $options: 'i' } }
      ];
    }
    const list = await farmIncomes.find(filter).sort({ date: -1, createdAt: -1 }).toArray();
    return res.json({ success: true, data: list.map(normalizeFarmIncome) });
  } catch (e) {
    console.error('List farm incomes error:', e);
    return res.status(500).json({ success: false, message: 'Failed to fetch incomes' });
  }
});

// GET one income by id
app.get("/api/farm/incomes/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const doc = await farmIncomes.findOne({ id: numericId });
    if (!doc) return res.status(404).json({ success: false, message: 'Income not found' });
    return res.json({ success: true, data: normalizeFarmIncome(doc) });
  } catch (e) {
    console.error('Get farm income error:', e);
    return res.status(500).json({ success: false, message: 'Failed to fetch income' });
  }
});

// UPDATE income by id
app.put("/api/farm/incomes/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const updateFields = {};
    const allowed = ['source', 'description', 'amount', 'date', 'paymentMethod', 'customer', 'notes'];
    for (const key of allowed) {
      if (req.body && Object.prototype.hasOwnProperty.call(req.body, key)) {
        updateFields[key] = key === 'amount' ? Number(req.body[key]) || 0 : String(req.body[key] ?? '');
      }
    }
    updateFields.updatedAt = new Date();

    const result = await farmIncomes.findOneAndUpdate(
      { id: numericId },
      { $set: updateFields },
      { returnDocument: 'after' }
    );
    if (!result.value) return res.status(404).json({ success: false, message: 'Income not found' });
    return res.json({ success: true, data: normalizeFarmIncome(result.value) });
  } catch (e) {
    console.error('Update farm income error:', e);
    return res.status(500).json({ success: false, message: 'Failed to update income' });
  }
});

// DELETE income by id
app.delete("/api/farm/incomes/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const numericId = Number(id);
    const del = await farmIncomes.deleteOne({ id: numericId });
    if (del.deletedCount === 0) return res.status(404).json({ success: false, message: 'Income not found' });
    return res.json({ success: true });
  } catch (e) {
    console.error('Delete farm income error:', e);
    return res.status(500).json({ success: false, message: 'Failed to delete income' });
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



// ==================== CURRENCY EXCHANGE ROUTES ====================

// POST: Create new currency exchange
app.post("/api/exchanges", async (req, res) => {
  try {
    const {
      date,
      fullName,
      mobileNumber,
      nid,
      type,
      currencyCode,
      currencyName,
      exchangeRate,
      quantity,
      amount_bdt
    } = req.body;

    // Validation
    if (!date || !fullName || !mobileNumber || !type || !currencyCode || !currencyName || !exchangeRate || !quantity) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'Date, full name, mobile number, type, currency code, currency name, exchange rate, and quantity are required'
      });
    }

    // Validate date format
    if (!isValidDate(date)) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'Invalid date format. Please use YYYY-MM-DD format'
      });
    }

    // Validate type
    if (!['Buy', 'Sell'].includes(type)) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'Type must be either "Buy" or "Sell"'
      });
    }

    // Validate exchange rate and quantity
    const rate = Number(exchangeRate);
    const qty = Number(quantity);

    if (!Number.isFinite(rate) || rate <= 0) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'Exchange rate must be a positive number'
      });
    }

    if (!Number.isFinite(qty) || qty <= 0) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'Quantity must be a positive number'
      });
    }

    // Calculate amount if not provided
    const calculatedAmount = rate * qty;

    // Create exchange document
    const exchangeData = {
      date,
      fullName: fullName.trim(),
      mobileNumber: mobileNumber.trim(),
      nid: nid ? nid.trim() : '',
      type,
      currencyCode,
      currencyName,
      exchangeRate: rate,
      quantity: qty,
      amount_bdt: amount_bdt || calculatedAmount,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await exchanges.insertOne(exchangeData);

    res.status(201).json({
      success: true,
      message: 'Exchange created successfully',
      exchange: { ...exchangeData, _id: result.insertedId }
    });

  } catch (error) {
    console.error('Create exchange error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to create exchange'
    });
  }
});

// GET: Get all exchanges with pagination and filters
app.get("/api/exchanges", async (req, res) => {
  try {
    const {
      page = 1,
      limit = 10,
      type,
      currencyCode,
      dateFrom,
      dateTo,
      search
    } = req.query;

    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    // Build query
    const query = { isActive: { $ne: false } };

    if (type && ['Buy', 'Sell'].includes(type)) {
      query.type = type;
    }

    if (currencyCode) {
      query.currencyCode = currencyCode;
    }

    if (dateFrom || dateTo) {
      query.date = {};
      if (dateFrom) query.date.$gte = dateFrom;
      if (dateTo) query.date.$lte = dateTo;
    }

    if (search) {
      query.$or = [
        { fullName: { $regex: search, $options: 'i' } },
        { mobileNumber: { $regex: search, $options: 'i' } },
        { nid: { $regex: search, $options: 'i' } }
      ];
    }

    // Get total count
    const total = await exchanges.countDocuments(query);

    // Get exchanges
    const exchangesList = await exchanges
      .find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum)
      .toArray();

    res.json({
      success: true,
      data: exchangesList,
      pagination: {
        page: pageNum,
        limit: limitNum,
        total,
        pages: Math.ceil(total / limitNum)
      }
    });

  } catch (error) {
    console.error('Get exchanges error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to fetch exchanges'
    });
  }
});

// GET: Get currency-wise reserves with purchase prices
app.get("/api/exchanges/reserves", async (req, res) => {
  try {
    // Get all active exchanges
    const allExchanges = await exchanges
      .find({ isActive: { $ne: false } })
      .sort({ date: 1, createdAt: 1 }) // Sort by date to maintain chronological order for FIFO
      .toArray();

    // Group by currency and calculate reserves using FIFO
    const currencyReserves = {};

    // Process each exchange using FIFO method
    for (const exchange of allExchanges) {
      const { currencyCode, currencyName, type, quantity, exchangeRate, amount_bdt } = exchange;

      if (!currencyCode) continue;

      // Initialize currency if not exists
      if (!currencyReserves[currencyCode]) {
        currencyReserves[currencyCode] = {
          currencyCode,
          currencyName: currencyName || currencyCode,
          inventory: [], // FIFO inventory: [{ quantity, purchaseRate, purchaseCost }]
          totalBought: 0,
          totalSold: 0,
          totalPurchaseCost: 0,
          totalSaleRevenue: 0
        };
      }

      const currency = currencyReserves[currencyCode];
      const qty = Number(quantity) || 0;
      const rate = Number(exchangeRate) || 0;
      const amount = Number(amount_bdt) || (rate * qty);

      if (type === 'Buy') {
        // Add to inventory (FIFO - add to end)
        currency.inventory.push({
          quantity: qty,
          purchaseRate: rate,
          purchaseCost: amount
        });
        currency.totalBought += qty;
        currency.totalPurchaseCost += amount;
      } else if (type === 'Sell') {
        // Remove from inventory using FIFO (remove from beginning)
        let remainingQty = qty;
        
        while (remainingQty > 0 && currency.inventory.length > 0) {
          const firstItem = currency.inventory[0];
          
          if (firstItem.quantity <= remainingQty) {
            // Use entire first item
            remainingQty -= firstItem.quantity;
            currency.inventory.shift(); // Remove from inventory
          } else {
            // Use partial first item
            const originalQuantity = firstItem.quantity;
            const costPerUnit = firstItem.purchaseCost / originalQuantity;
            const usedCost = costPerUnit * remainingQty;
            firstItem.purchaseCost -= usedCost;
            firstItem.quantity -= remainingQty;
            remainingQty = 0;
          }
        }

        currency.totalSold += qty;
        currency.totalSaleRevenue += amount;
      }
    }

    // Calculate reserves and purchase prices from remaining inventory
    const reservesArray = Object.values(currencyReserves).map(currency => {
      // Calculate current reserve from remaining inventory
      const reserve = currency.inventory.reduce((sum, item) => sum + item.quantity, 0);
      const reserveCost = currency.inventory.reduce((sum, item) => sum + item.purchaseCost, 0);
      
      // Calculate weighted average purchase price of current reserve
      let weightedAveragePurchasePrice = 0;
      if (reserve > 0) {
        weightedAveragePurchasePrice = reserveCost / reserve;
      }

      // Calculate current reserve value
      const currentReserveValue = reserve * weightedAveragePurchasePrice;

      return {
        currencyCode: currency.currencyCode,
        currencyName: currency.currencyName,
        totalBought: currency.totalBought,
        totalSold: currency.totalSold,
        reserve: reserve,
        weightedAveragePurchasePrice: weightedAveragePurchasePrice,
        currentReserveValue: currentReserveValue,
        totalPurchaseCost: currency.totalPurchaseCost,
        totalSaleRevenue: currency.totalSaleRevenue
      };
    });

    // Filter out currencies with zero reserves
    const activeReserves = reservesArray.filter(c => c.reserve > 0);

    res.json({
      success: true,
      data: activeReserves,
      summary: {
        totalCurrencies: activeReserves.length,
        totalReserveValue: activeReserves.reduce((sum, c) => sum + c.currentReserveValue, 0)
      }
    });

  } catch (error) {
    console.error('Get reserves error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to fetch currency reserves'
    });
  }
});

// GET: Get profit/loss dashboard for money exchange
app.get("/api/exchanges/dashboard", async (req, res) => {
  try {
    const { currencyCode, fromDate, toDate } = req.query;

    // Build query
    const query = { isActive: { $ne: false } };
    if (currencyCode) {
      query.currencyCode = currencyCode;
    }
    if (fromDate || toDate) {
      query.date = {};
      if (fromDate) query.date.$gte = fromDate;
      if (toDate) query.date.$lte = toDate;
    }

    // Get all exchanges
    const allExchanges = await exchanges
      .find(query)
      .sort({ date: 1, createdAt: 1 })
      .toArray();

    // Calculate reserves and profit/loss by currency
    const currencyData = {};
    let totalRealizedProfitLoss = 0;
    let totalUnrealizedProfitLoss = 0;

    // Process each exchange to calculate cost basis using FIFO method
    for (const exchange of allExchanges) {
      const { currencyCode, currencyName, type, quantity, exchangeRate, amount_bdt } = exchange;

      if (!currencyCode) continue;

      // Initialize currency if not exists
      if (!currencyData[currencyCode]) {
        currencyData[currencyCode] = {
          currencyCode,
          currencyName: currencyName || currencyCode,
          inventory: [], // FIFO inventory: [{ quantity, purchaseRate, purchaseCost }]
          totalBought: 0,
          totalSold: 0,
          totalPurchaseCost: 0,
          totalSaleRevenue: 0,
          realizedProfitLoss: 0,
          currentReserve: 0,
          weightedAveragePurchasePrice: 0,
          currentReserveValue: 0
        };
      }

      const currency = currencyData[currencyCode];
      const qty = Number(quantity) || 0;
      const rate = Number(exchangeRate) || 0;
      const amount = Number(amount_bdt) || (rate * qty);

      if (type === 'Buy') {
        // Add to inventory
        currency.inventory.push({
          quantity: qty,
          purchaseRate: rate,
          purchaseCost: amount
        });
        currency.totalBought += qty;
        currency.totalPurchaseCost += amount;
      } else if (type === 'Sell') {
        let remainingQty = qty;
        let costOfGoodsSold = 0;

        // FIFO: Remove from inventory
        while (remainingQty > 0 && currency.inventory.length > 0) {
          const firstItem = currency.inventory[0];
          
          if (firstItem.quantity <= remainingQty) {
            // Use entire first item
            costOfGoodsSold += firstItem.purchaseCost;
            remainingQty -= firstItem.quantity;
            currency.inventory.shift(); // Remove from inventory
          } else {
            // Use partial first item
            const costPerUnit = firstItem.purchaseCost / firstItem.quantity;
            const usedCost = costPerUnit * remainingQty;
            costOfGoodsSold += usedCost;
            firstItem.quantity -= remainingQty;
            firstItem.purchaseCost -= usedCost;
            remainingQty = 0;
          }
        }

        // Calculate profit/loss for this sale
        const saleRevenue = amount;
        const profitLoss = saleRevenue - costOfGoodsSold;
        currency.realizedProfitLoss += profitLoss;
        currency.totalSold += qty;
        currency.totalSaleRevenue += amount;
      }
    }

    // Calculate final reserves and unrealized profit/loss
    const dashboardData = Object.values(currencyData).map(currency => {
      // Calculate current reserve from inventory
      currency.currentReserve = currency.inventory.reduce((sum, item) => sum + item.quantity, 0);
      
      // Calculate weighted average purchase price of current reserve
      const totalReserveCost = currency.inventory.reduce((sum, item) => sum + item.purchaseCost, 0);
      if (currency.currentReserve > 0) {
        currency.weightedAveragePurchasePrice = totalReserveCost / currency.currentReserve;
      }

      // Current reserve value (using weighted average)
      currency.currentReserveValue = currency.currentReserve * currency.weightedAveragePurchasePrice;

      // Calculate average sell rate for comparison (optional)
      const avgSellRate = currency.totalSold > 0 
        ? currency.totalSaleRevenue / currency.totalSold 
        : 0;

      currency.averageSellRate = avgSellRate;
      currency.unrealizedProfitLoss = 0; // Can be calculated if current market rate is provided

      // Remove inventory details from response
      delete currency.inventory;

      totalRealizedProfitLoss += currency.realizedProfitLoss;

      return currency;
    });

    // Overall summary
    const summary = {
      totalRealizedProfitLoss,
      totalUnrealizedProfitLoss,
      totalPurchaseCost: dashboardData.reduce((sum, c) => sum + c.totalPurchaseCost, 0),
      totalSaleRevenue: dashboardData.reduce((sum, c) => sum + c.totalSaleRevenue, 0),
      totalCurrentReserveValue: dashboardData.reduce((sum, c) => sum + c.currentReserveValue, 0),
      totalCurrencies: dashboardData.length
    };

    res.json({
      success: true,
      data: dashboardData,
      summary
    });

  } catch (error) {
    console.error('Get dashboard error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to fetch dashboard data'
    });
  }
});

// GET: Get single exchange by ID
app.get("/api/exchanges/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid ID',
        message: 'Invalid exchange ID format'
      });
    }

    const exchange = await exchanges.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!exchange) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Exchange not found'
      });
    }

    res.json({
      success: true,
      exchange
    });

  } catch (error) {
    console.error('Get exchange error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to fetch exchange'
    });
  }
});

// PUT: Update exchange by ID
app.put("/api/exchanges/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const {
      date,
      fullName,
      mobileNumber,
      nid,
      type,
      currencyCode,
      currencyName,
      exchangeRate,
      quantity,
      amount_bdt
    } = req.body;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid ID',
        message: 'Invalid exchange ID format'
      });
    }

    // Check if exchange exists
    const existingExchange = await exchanges.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!existingExchange) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Exchange not found'
      });
    }

    // Build update object
    const updateData = {
      updatedAt: new Date()
    };

    if (date !== undefined) {
      if (!isValidDate(date)) {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Invalid date format. Please use YYYY-MM-DD format'
        });
      }
      updateData.date = date;
    }

    if (fullName !== undefined) {
      if (!fullName || fullName.trim() === '') {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Full name is required'
        });
      }
      updateData.fullName = fullName.trim();
    }

    if (mobileNumber !== undefined) {
      if (!mobileNumber || mobileNumber.trim() === '') {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Mobile number is required'
        });
      }
      updateData.mobileNumber = mobileNumber.trim();
    }

    if (nid !== undefined) {
      updateData.nid = nid ? nid.trim() : '';
    }

    if (type !== undefined) {
      if (!['Buy', 'Sell'].includes(type)) {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Type must be either "Buy" or "Sell"'
        });
      }
      updateData.type = type;
    }

    if (currencyCode !== undefined) {
      updateData.currencyCode = currencyCode;
    }

    if (currencyName !== undefined) {
      updateData.currencyName = currencyName;
    }

    if (exchangeRate !== undefined) {
      const rate = Number(exchangeRate);
      if (!Number.isFinite(rate) || rate <= 0) {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Exchange rate must be a positive number'
        });
      }
      updateData.exchangeRate = rate;
    }

    if (quantity !== undefined) {
      const qty = Number(quantity);
      if (!Number.isFinite(qty) || qty <= 0) {
        return res.status(400).json({
          success: false,
          error: 'Validation error',
          message: 'Quantity must be a positive number'
        });
      }
      updateData.quantity = qty;
    }

    // Recalculate amount if exchangeRate or quantity changed
    if (updateData.exchangeRate !== undefined || updateData.quantity !== undefined) {
      const finalRate = updateData.exchangeRate !== undefined ? updateData.exchangeRate : existingExchange.exchangeRate;
      const finalQty = updateData.quantity !== undefined ? updateData.quantity : existingExchange.quantity;
      updateData.amount_bdt = finalRate * finalQty;
    } else if (amount_bdt !== undefined) {
      updateData.amount_bdt = Number(amount_bdt);
    }

    // Update exchange
    const result = await exchanges.updateOne(
      { _id: new ObjectId(id) },
      { $set: updateData }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Exchange not found'
      });
    }

    // Get updated exchange
    const updatedExchange = await exchanges.findOne({ _id: new ObjectId(id) });

    res.json({
      success: true,
      message: 'Exchange updated successfully',
      exchange: updatedExchange
    });

  } catch (error) {
    console.error('Update exchange error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to update exchange'
    });
  }
});

// DELETE: Delete exchange by ID (soft delete)
app.delete("/api/exchanges/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid ID',
        message: 'Invalid exchange ID format'
      });
    }

    // Check if exchange exists
    const existingExchange = await exchanges.findOne({
      _id: new ObjectId(id),
      isActive: { $ne: false }
    });

    if (!existingExchange) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Exchange not found'
      });
    }

    // Soft delete
    const result = await exchanges.updateOne(
      { _id: new ObjectId(id) },
      {
        $set: {
          isActive: false,
          updatedAt: new Date()
        }
      }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Exchange not found'
      });
    }

    res.json({
      success: true,
      message: 'Exchange deleted successfully'
    });

  } catch (error) {
    console.error('Delete exchange error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to delete exchange'
    });
  }
});

// ==================== NOTIFICATIONS ====================
// Create a notification
app.post("/api/notifications", async (req, res) => {
  try {
    const { userId, title, message, type = 'info', link, metadata } = req.body || {};

    if (!userId || !title || !message) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'userId, title and message are required'
      });
    }

    const normalizedUserId = ObjectId.isValid(userId) ? new ObjectId(userId) : userId;
    const doc = {
      userId: normalizedUserId,
      title,
      message,
      type,
      link: link || null,
      metadata: metadata || null,
      isRead: false,
      isActive: true,
      createdAt: new Date(),
      updatedAt: new Date()
    };

    const result = await notifications.insertOne(doc);

    return res.status(201).json({
      success: true,
      notification: { ...doc, _id: result.insertedId }
    });
  } catch (error) {
    console.error('Create notification error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to create notification'
    });
  }
});

// Fetch notifications for a user
app.get("/api/notifications", async (req, res) => {
  try {
    const { userId, isRead, limit = 20, skip = 0 } = req.query;

    const baseFilter = { isActive: { $ne: false } };
    if (userId) {
      baseFilter.userId = ObjectId.isValid(userId) ? new ObjectId(userId) : userId;
    }

    const filter = { ...baseFilter };
    if (isRead === 'true') filter.isRead = true;
    if (isRead === 'false') filter.isRead = false;

    const parsedLimit = Math.min(parseInt(limit, 10) || 20, 100);
    const parsedSkip = parseInt(skip, 10) || 0;

    const [items, unreadCount] = await Promise.all([
      notifications.find(filter)
        .sort({ createdAt: -1 })
        .skip(parsedSkip)
        .limit(parsedLimit)
        .toArray(),
      notifications.countDocuments({ ...baseFilter, isRead: false })
    ]);

    res.json({
      success: true,
      notifications: items,
      unreadCount,
      pagination: {
        limit: parsedLimit,
        skip: parsedSkip,
        returned: items.length
      }
    });
  } catch (error) {
    console.error('Get notifications error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to fetch notifications'
    });
  }
});

// Mark a single notification as read
app.patch("/api/notifications/:id/read", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid ID',
        message: 'Invalid notification id'
      });
    }

    const result = await notifications.updateOne(
      { _id: new ObjectId(id), isActive: { $ne: false } },
      { $set: { isRead: true, readAt: new Date(), updatedAt: new Date() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Notification not found'
      });
    }

    res.json({
      success: true,
      message: 'Notification marked as read'
    });
  } catch (error) {
    console.error('Mark notification read error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to update notification'
    });
  }
});

// Mark all notifications as read for a user
app.patch("/api/notifications/read-all", async (req, res) => {
  try {
    const { userId } = req.body || {};

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'Validation error',
        message: 'userId is required'
      });
    }

    const normalizedUserId = ObjectId.isValid(userId) ? new ObjectId(userId) : userId;
    const result = await notifications.updateMany(
      { userId: normalizedUserId, isRead: { $ne: true }, isActive: { $ne: false } },
      { $set: { isRead: true, readAt: new Date(), updatedAt: new Date() } }
    );

    res.json({
      success: true,
      message: 'All notifications marked as read',
      updatedCount: result.modifiedCount
    });
  } catch (error) {
    console.error('Mark all notifications read error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to update notifications'
    });
  }
});

// Soft delete a notification
app.delete("/api/notifications/:id", async (req, res) => {
  try {
    const { id } = req.params;

    if (!ObjectId.isValid(id)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid ID',
        message: 'Invalid notification id'
      });
    }

    const result = await notifications.updateOne(
      { _id: new ObjectId(id), isActive: { $ne: false } },
      { $set: { isActive: false, updatedAt: new Date() } }
    );

    if (result.matchedCount === 0) {
      return res.status(404).json({
        success: false,
        error: 'Not found',
        message: 'Notification not found'
      });
    }

    res.json({
      success: true,
      message: 'Notification deleted successfully'
    });
  } catch (error) {
    console.error('Delete notification error:', error);
    res.status(500).json({
      success: false,
      error: 'Internal server error',
      message: 'Failed to delete notification'
    });
  }
});

// ==================== DASHBOARD SUMMARY ENDPOINT ====================
// Comprehensive dashboard summary with all module statistics
app.get("/api/dashboard/summary", async (req, res) => {
  try {
    // Check if database collections are initialized
    if (!db || !users || !airCustomers || !agents || !vendors || !branches || 
        !transactions || !invoices || !accounts || !bankAccounts || !loans || 
        !orders || !packages || !agentPackages || !tickets || !exchanges || 
        !cattle || !milkProductions || !farmEmployees || !farmExpenses || 
        !farmIncomes || !haji || !umrah) {
      return res.status(503).json({
        success: false,
        error: 'Database not initialized',
        message: 'Database collections are not available. Please try again in a moment.'
      });
    }

    const { fromDate, toDate } = req.query || {};
    
    // Build date filter
    const dateFilter = {};
    if (fromDate || toDate) {
      dateFilter.createdAt = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (isNaN(start.getTime())) {
          return res.status(400).json({
            success: false,
            error: 'Invalid date format',
            message: 'fromDate must be a valid date string'
          });
        }
        dateFilter.createdAt.$gte = start;
      }
      if (toDate) {
        const end = new Date(toDate);
        if (isNaN(end.getTime())) {
          return res.status(400).json({
            success: false,
            error: 'Invalid date format',
            message: 'toDate must be a valid date string'
          });
        }
        end.setHours(23, 59, 59, 999);
        dateFilter.createdAt.$lte = end;
      }
    }

    // Date filter for transactions
    const transactionDateFilter = {};
    if (fromDate || toDate) {
      transactionDateFilter.date = {};
      if (fromDate) {
        const start = new Date(fromDate);
        if (!isNaN(start.getTime())) {
          transactionDateFilter.date.$gte = start;
        }
      }
      if (toDate) {
        const end = new Date(toDate);
        if (!isNaN(end.getTime())) {
          end.setHours(23, 59, 59, 999);
          transactionDateFilter.date.$lte = end;
        }
      }
    }

    // Initialize summary object
    const summary = {
      overview: {},
      users: {},
      customers: {},
      agents: {},
      vendors: {},
      financial: {},
      services: {},
      farm: {},
      recentActivity: {}
    };

    // ==================== OVERVIEW ====================
    const totalUsers = await users.countDocuments({ isActive: { $ne: false } });
    const totalBranches = await branches.countDocuments({ isActive: { $ne: false } });
    const totalCustomers = await airCustomers.countDocuments({ isActive: { $ne: false } });
    const totalAgents = await agents.countDocuments({ isActive: { $ne: false } });
    const totalVendors = await vendors.countDocuments({ isActive: { $ne: false } });
    
    summary.overview = {
      totalUsers,
      totalBranches,
      totalCustomers,
      totalAgents,
      totalVendors
    };

    // ==================== USERS ====================
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const thisMonth = new Date();
    thisMonth.setDate(1);
    thisMonth.setHours(0, 0, 0, 0);

    const usersToday = await users.countDocuments({
      createdAt: { $gte: today },
      isActive: { $ne: false }
    });
    const usersThisMonth = await users.countDocuments({
      createdAt: { $gte: thisMonth },
      isActive: { $ne: false }
    });

    // Users by role
    const usersByRole = await users.aggregate([
      { $match: { isActive: { $ne: false } } },
      { $group: { _id: "$role", count: { $sum: 1 } } }
    ]).toArray();

    summary.users = {
      total: totalUsers,
      today: usersToday,
      thisMonth: usersThisMonth,
      byRole: usersByRole.reduce((acc, item) => {
        acc[item._id || 'unknown'] = item.count;
        return acc;
      }, {})
    };

    // ==================== CUSTOMERS ====================
    const customersToday = await airCustomers.countDocuments({
      createdAt: { $gte: today },
      isActive: { $ne: false }
    });
    const customersThisMonth = await airCustomers.countDocuments({
      createdAt: { $gte: thisMonth },
      isActive: { $ne: false }
    });

    // Customers by type
    const customersByType = await airCustomers.aggregate([
      { $match: { isActive: { $ne: false } } },
      { $group: { _id: "$customerType", count: { $sum: 1 } } }
    ]).toArray();

    // Haji customers
    const hajiCustomers = await haji.countDocuments({ isActive: { $ne: false } });
    const umrahCustomers = await umrah.countDocuments({ isActive: { $ne: false } });

    // Haji payment stats (for profit calculation)
    const hajiPaymentStats = await haji.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const hajiPayStats = hajiPaymentStats[0] || { totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    // Umrah payment stats (for profit calculation)
    const umrahPaymentStats = await umrah.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const umrahPayStats = umrahPaymentStats[0] || { totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    // Customer payment stats
    const customerPaymentStats = await airCustomers.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const paymentStats = customerPaymentStats[0] || { totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    summary.customers = {
      total: totalCustomers,
      today: customersToday,
      thisMonth: customersThisMonth,
      haji: hajiCustomers,
      umrah: umrahCustomers,
      byType: customersByType.reduce((acc, item) => {
        acc[item._id || 'unknown'] = item.count;
        return acc;
      }, {}),
      paymentStats: {
        totalAmount: parseFloat((paymentStats.totalAmount || 0).toFixed(2)),
        paidAmount: parseFloat((paymentStats.paidAmount || 0).toFixed(2)),
        dueAmount: parseFloat((paymentStats.dueAmount || 0).toFixed(2))
      }
    };

    // ==================== AGENTS ====================
    const agentsToday = await agents.countDocuments({
      createdAt: { $gte: today },
      isActive: { $ne: false }
    });
    const agentsThisMonth = await agents.countDocuments({
      createdAt: { $gte: thisMonth },
      isActive: { $ne: false }
    });

    // Agent payment stats
    const agentPaymentStats = await agents.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const agentPayStats = agentPaymentStats[0] || { totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    summary.agents = {
      total: totalAgents,
      today: agentsToday,
      thisMonth: agentsThisMonth,
      paymentStats: {
        totalAmount: parseFloat((agentPayStats.totalAmount || 0).toFixed(2)),
        paidAmount: parseFloat((agentPayStats.paidAmount || 0).toFixed(2)),
        dueAmount: parseFloat((agentPayStats.dueAmount || 0).toFixed(2))
      }
    };

    // ==================== VENDORS ====================
    const vendorsToday = await vendors.countDocuments({
      createdAt: { $gte: today },
      isActive: { $ne: false }
    });
    const vendorsThisMonth = await vendors.countDocuments({
      createdAt: { $gte: thisMonth },
      isActive: { $ne: false }
    });

    // Vendor payment stats
    const vendorPaymentStats = await vendors.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const vendorPayStats = vendorPaymentStats[0] || { totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    summary.vendors = {
      total: totalVendors,
      today: vendorsToday,
      thisMonth: vendorsThisMonth,
      paymentStats: {
        totalAmount: parseFloat((vendorPayStats.totalAmount || 0).toFixed(2)),
        paidAmount: parseFloat((vendorPayStats.paidAmount || 0).toFixed(2)),
        dueAmount: parseFloat((vendorPayStats.dueAmount || 0).toFixed(2))
      }
    };

    // ==================== FINANCIAL ====================
    // Transactions
    const transactionMatch = {
      ...transactionDateFilter,
      isActive: { $ne: false },
      status: 'completed'
    };

    const transactionStats = await transactions.aggregate([
      { $match: transactionMatch },
      {
        $group: {
          _id: "$transactionType",
          totalAmount: { $sum: { $ifNull: ["$amount", 0] } },
          count: { $sum: 1 }
        }
      }
    ]).toArray();

    let totalCredit = 0;
    let totalDebit = 0;
    let creditCount = 0;
    let debitCount = 0;

    transactionStats.forEach(stat => {
      if (stat._id === 'credit') {
        totalCredit = stat.totalAmount;
        creditCount = stat.count;
      } else if (stat._id === 'debit') {
        totalDebit = stat.totalAmount;
        debitCount = stat.count;
      }
    });

    // Invoices
    const invoiceMatch = {
      ...dateFilter,
      isActive: { $ne: false }
    };

    const invoiceStats = await invoices.aggregate([
      { $match: invoiceMatch },
      {
        $group: {
          _id: null,
          totalInvoices: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$total", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paid", 0] } },
          dueAmount: { $sum: { $ifNull: ["$due", 0] } }
        }
      }
    ]).toArray();

    const invoiceStat = invoiceStats[0] || { totalInvoices: 0, totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    // Accounts (balance)
    const accountStats = await accounts.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalAccounts: { $sum: 1 },
          totalBalance: { $sum: { $ifNull: ["$balance", 0] } }
        }
      }
    ]).toArray();

    const accountStat = accountStats[0] || { totalAccounts: 0, totalBalance: 0 };

    // Bank Accounts
    const bankAccountStats = await bankAccounts.aggregate([
      { $match: { isDeleted: { $ne: true }, status: 'Active' } },
      {
        $group: {
          _id: null,
          totalBankAccounts: { $sum: 1 },
          totalBalance: { $sum: { $ifNull: ["$currentBalance", 0] } }
        }
      }
    ]).toArray();

    const bankAccountStat = bankAccountStats[0] || { totalBankAccounts: 0, totalBalance: 0 };

    // Loans
    const loanStats = await loans.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalLoans: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$totalAmount", 0] } },
          paidAmount: { $sum: { $ifNull: ["$paidAmount", 0] } },
          dueAmount: { $sum: { $ifNull: ["$totalDue", 0] } }
        }
      }
    ]).toArray();

    const loanStat = loanStats[0] || { totalLoans: 0, totalAmount: 0, paidAmount: 0, dueAmount: 0 };

    // Orders
    const totalOrders = await orders.countDocuments({ isActive: { $ne: false } });
    const ordersToday = await orders.countDocuments({
      createdAt: { $gte: today },
      isActive: { $ne: false }
    });

    summary.financial = {
      transactions: {
        totalCredit: parseFloat(totalCredit.toFixed(2)),
        totalDebit: parseFloat(totalDebit.toFixed(2)),
        netAmount: parseFloat((totalCredit - totalDebit).toFixed(2)),
        creditCount,
        debitCount,
        totalCount: creditCount + debitCount
      },
      invoices: {
        totalInvoices: invoiceStat.totalInvoices,
        totalAmount: parseFloat((invoiceStat.totalAmount || 0).toFixed(2)),
        paidAmount: parseFloat((invoiceStat.paidAmount || 0).toFixed(2)),
        dueAmount: parseFloat((invoiceStat.dueAmount || 0).toFixed(2))
      },
      accounts: {
        totalAccounts: accountStat.totalAccounts,
        totalBalance: parseFloat((accountStat.totalBalance || 0).toFixed(2))
      },
      bankAccounts: {
        totalBankAccounts: bankAccountStat.totalBankAccounts,
        totalBalance: parseFloat((bankAccountStat.totalBalance || 0).toFixed(2))
      },
      loans: {
        totalLoans: loanStat.totalLoans,
        totalAmount: parseFloat((loanStat.totalAmount || 0).toFixed(2)),
        paidAmount: parseFloat((loanStat.paidAmount || 0).toFixed(2)),
        dueAmount: parseFloat((loanStat.dueAmount || 0).toFixed(2))
      },
      orders: {
        total: totalOrders,
        today: ordersToday
      }
    };

    // ==================== SERVICES ====================
    // Packages
    const totalPackages = await packages.countDocuments({ isActive: { $ne: false } });
    const agentPackagesCount = await agentPackages.countDocuments({ isActive: { $ne: false } });

    // Tickets
    const ticketStats = await tickets.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalTickets: { $sum: 1 },
          totalAmount: { $sum: { $ifNull: ["$totalFare", 0] } }
        }
      }
    ]).toArray();

    const ticketStat = ticketStats[0] || { totalTickets: 0, totalAmount: 0 };

    // Exchanges
    const exchangeStats = await exchanges.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: "$type",
          totalAmount: { $sum: { $ifNull: ["$amount_bdt", 0] } },
          count: { $sum: 1 }
        }
      }
    ]).toArray();

    let exchangeBuy = 0;
    let exchangeSell = 0;
    let exchangeBuyCount = 0;
    let exchangeSellCount = 0;

    exchangeStats.forEach(stat => {
      if (stat._id === 'Buy') {
        exchangeBuy = stat.totalAmount;
        exchangeBuyCount = stat.count;
      } else if (stat._id === 'Sell') {
        exchangeSell = stat.totalAmount;
        exchangeSellCount = stat.count;
      }
    });

    summary.services = {
      packages: {
        total: totalPackages,
        agentPackages: agentPackagesCount
      },
      tickets: {
        total: ticketStat.totalTickets,
        totalAmount: parseFloat((ticketStat.totalAmount || 0).toFixed(2))
      },
      exchanges: {
        buyAmount: parseFloat(exchangeBuy.toFixed(2)),
        sellAmount: parseFloat(exchangeSell.toFixed(2)),
        buyCount: exchangeBuyCount,
        sellCount: exchangeSellCount,
        netAmount: parseFloat((exchangeSell - exchangeBuy).toFixed(2))
      }
    };

    // ==================== FARM ====================
    // Cattle
    const cattleStats = await cattle.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 }
        }
      }
    ]).toArray();

    const totalCattle = await cattle.countDocuments({ isActive: { $ne: false } });
    const cattleByStatus = cattleStats.reduce((acc, item) => {
      acc[item._id || 'unknown'] = item.count;
      return acc;
    }, {});

    // Milk Production
    const milkStats = await milkProductions.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalProduction: { $sum: { $ifNull: ["$quantity", 0] } },
          totalCount: { $sum: 1 }
        }
      }
    ]).toArray();

    const milkStat = milkStats[0] || { totalProduction: 0, totalCount: 0 };

    // Farm Employees
    const farmEmployeeStats = await farmEmployees.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: "$status",
          count: { $sum: 1 },
          totalSalary: { $sum: { $ifNull: ["$salary", 0] } }
        }
      }
    ]).toArray();

    let totalFarmEmployees = 0;
    let activeFarmEmployees = 0;
    let totalFarmSalary = 0;

    farmEmployeeStats.forEach(stat => {
      totalFarmEmployees += stat.count;
      if (stat._id === 'active') {
        activeFarmEmployees = stat.count;
        totalFarmSalary = stat.totalSalary;
      }
    });

    // Farm Expenses
    const farmExpenseStats = await farmExpenses.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalExpenses: { $sum: { $ifNull: ["$amount", 0] } },
          totalCount: { $sum: 1 }
        }
      }
    ]).toArray();

    const farmExpenseStat = farmExpenseStats[0] || { totalExpenses: 0, totalCount: 0 };

    // Farm Incomes
    const farmIncomeStats = await farmIncomes.aggregate([
      { $match: { isActive: { $ne: false } } },
      {
        $group: {
          _id: null,
          totalIncomes: { $sum: { $ifNull: ["$amount", 0] } },
          totalCount: { $sum: 1 }
        }
      }
    ]).toArray();

    const farmIncomeStat = farmIncomeStats[0] || { totalIncomes: 0, totalCount: 0 };

    summary.farm = {
      cattle: {
        total: totalCattle,
        byStatus: cattleByStatus
      },
      milkProduction: {
        totalProduction: parseFloat((milkStat.totalProduction || 0).toFixed(2)),
        totalRecords: milkStat.totalCount
      },
      employees: {
        total: totalFarmEmployees,
        active: activeFarmEmployees,
        totalSalary: parseFloat((totalFarmSalary || 0).toFixed(2))
      },
      expenses: {
        total: parseFloat((farmExpenseStat.totalExpenses || 0).toFixed(2)),
        totalRecords: farmExpenseStat.totalCount
      },
      incomes: {
        total: parseFloat((farmIncomeStat.totalIncomes || 0).toFixed(2)),
        totalRecords: farmIncomeStat.totalCount
      },
      netProfit: parseFloat(((farmIncomeStat.totalIncomes || 0) - (farmExpenseStat.totalExpenses || 0)).toFixed(2))
    };

    // ==================== RECENT ACTIVITY ====================
    const recentTransactions = await transactions.find({
      isActive: { $ne: false },
      status: 'completed'
    })
      .sort({ createdAt: -1 })
      .limit(5)
      .toArray();

    const recentCustomers = await airCustomers.find({
      isActive: { $ne: false }
    })
      .sort({ createdAt: -1 })
      .limit(5)
      .toArray();

    const recentInvoices = await invoices.find({
      isActive: { $ne: false }
    })
      .sort({ createdAt: -1 })
      .limit(5)
      .toArray();

    summary.recentActivity = {
      transactions: recentTransactions.map(tx => ({
        transactionId: tx.transactionId,
        transactionType: tx.transactionType,
        amount: tx.amount,
        partyType: tx.partyType,
        createdAt: tx.createdAt
      })),
      customers: recentCustomers.map(c => ({
        customerId: c.customerId,
        name: c.name,
        customerType: c.customerType,
        createdAt: c.createdAt
      })),
      invoices: recentInvoices.map(inv => ({
        invoiceId: inv.invoiceId,
        total: inv.total,
        paid: inv.paid,
        due: inv.due,
        createdAt: inv.createdAt
      }))
    };

    // ==================== GRAND TOTALS ====================
    // Profit/Loss breakdown (includes all major income/expense streams)
    // Individual Dashboard Profits (these are added to netProfit):
    const airCustomerProfit = Number((paymentStats.paidAmount || 0).toFixed(2));
    const hajiProfit = Number((hajiPayStats.paidAmount || 0).toFixed(2));
    const umrahProfit = Number((umrahPayStats.paidAmount || 0).toFixed(2));
    const moneyExchangeProfit = Number((exchangeSell - exchangeBuy).toFixed(2));

    const incomeBreakdown = {
      transactionCredit: Number(totalCredit.toFixed(2)),
      invoicePaid: Number((invoiceStat.paidAmount || 0).toFixed(2)),
      // Individual Dashboard Profits
      airCustomerProfit: airCustomerProfit,
      hajiProfit: hajiProfit,
      umrahProfit: umrahProfit,
      moneyExchangeProfit: moneyExchangeProfit,
      // Other income sources
      farmIncomes: Number((farmIncomeStat.totalIncomes || 0).toFixed(2)),
      ticketSales: Number((ticketStat.totalAmount || 0).toFixed(2))
    };

    const expenseBreakdown = {
      transactionDebit: Number(totalDebit.toFixed(2)),
      // exchangeBuy is already accounted for in moneyExchangeProfit (exchangeSell - exchangeBuy)
      farmExpenses: Number((farmExpenseStat.totalExpenses || 0).toFixed(2)),
      farmSalaries: Number((totalFarmSalary || 0).toFixed(2))
    };

    const totalRevenue = Number(
      Object.values(incomeBreakdown).reduce((sum, val) => sum + (val || 0), 0).toFixed(2)
    );
    const totalExpenses = Number(
      Object.values(expenseBreakdown).reduce((sum, val) => sum + (val || 0), 0).toFixed(2)
    );
    const netProfitLoss = Number((totalRevenue - totalExpenses).toFixed(2));

    const grandTotals = {
      totalRevenue,
      totalExpenses,
      totalDue: parseFloat((
        (paymentStats.dueAmount || 0) +
        (agentPayStats.dueAmount || 0) +
        (vendorPayStats.dueAmount || 0) +
        (invoiceStat.dueAmount || 0) +
        (loanStat.dueAmount || 0)
      ).toFixed(2)),
      totalAssets: parseFloat((
        (accountStat.totalBalance || 0) +
        (bankAccountStat.totalBalance || 0)
      ).toFixed(2)),
      netProfit: netProfitLoss,
      profitLoss: {
        totalIncome: totalRevenue,
        totalExpenses,
        netProfit: netProfitLoss,
        incomeBreakdown,
        expenseBreakdown
      }
    };

    // Response
    res.json({
      success: true,
      data: summary,
      grandTotals,
      period: {
        fromDate: fromDate || null,
        toDate: toDate || null
      },
      generatedAt: new Date()
    });

  } catch (error) {
    console.error('Dashboard summary error:', error);
    console.error('Error stack:', error.stack);
    
    // Provide more specific error messages
    let errorMessage = 'Failed to generate dashboard summary';
    let statusCode = 500;
    
    if (error.message && error.message.includes('not defined')) {
      errorMessage = 'Database collection not initialized';
      statusCode = 503;
    } else if (error.message && error.message.includes('connection')) {
      errorMessage = 'Database connection error';
      statusCode = 503;
    } else if (error.message && error.message.includes('timeout')) {
      errorMessage = 'Request timeout - database query took too long';
      statusCode = 504;
    }
    
    res.status(statusCode).json({
      success: false,
      error: 'Internal server error',
      message: errorMessage,
      details: process.env.NODE_ENV === 'development' ? error.message : 'An error occurred while processing your request',
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
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
