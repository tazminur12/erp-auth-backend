# Loan Transaction Setup Guide (বাংলায়)

## 🎯 Frontend Developer এর জন্য Complete Guide

### 📋 Overview
Loan system এ transaction track করার জন্য যা যা দরকার সব এই guide এ আছে।

---

## 1️⃣ Loan Giving এর সময় Transaction Setup

### ✅ যা যা আছে (Already Implemented):
- Loan create হলে automatically transaction create হয় (যদি `targetAccountId` দেওয়া থাকে)
- Bank account balance update হয়

### 📝 Frontend এ যা করতে হবে:

#### Loan Giving Form এ:
```javascript
// Loan giving form এ bank account select field যোগ করুন
const [selectedAccountId, setSelectedAccountId] = useState('');

// Form submit করার সময়:
const handleSubmit = async (e) => {
  e.preventDefault();
  
  const loanData = {
    // ... সব loan fields
    fullName: formData.fullName,
    amount: formData.amount,
    // ... বাকি সব fields
    
    // ✅ এই field টি অবশ্যই add করতে হবে:
    targetAccountId: selectedAccountId, // কোন account থেকে money debit হবে
    
    createdBy: userProfile?.email,
    branchId: userProfile?.branchId
  };
  
  try {
    const response = await axiosSecure.post('/loans/giving', loanData);
    
    if (response.data.success) {
      // Success! Transaction automatically create হয়েছে
      console.log('Transaction ID:', response.data.transactionId);
    }
  } catch (error) {
    console.error('Error:', error);
  }
};
```

#### Bank Account Select Component:
```jsx
// Bank accounts fetch করুন
const [bankAccounts, setBankAccounts] = useState([]);

useEffect(() => {
  const fetchAccounts = async () => {
    try {
      const response = await axiosSecure.get('/bank-accounts');
      if (response.data.success) {
        setBankAccounts(response.data.accounts || []);
      }
    } catch (error) {
      console.error('Error fetching accounts:', error);
    }
  };
  fetchAccounts();
}, []);

// Form এ select field:
<select
  name="targetAccountId"
  value={selectedAccountId}
  onChange={(e) => setSelectedAccountId(e.target.value)}
  required
>
  <option value="">Select Bank Account</option>
  {bankAccounts
    .filter(acc => acc.status === 'Active' && !acc.isDeleted)
    .map(account => (
      <option key={account._id} value={account._id}>
        {account.bankName} - {account.accountNumber} 
        (Balance: {account.currentBalance} {account.currency})
      </option>
    ))
  }
</select>
```

---

## 2️⃣ Loan Payment Track করা (Borrower Payment ফেরত দিচ্ছে)

### ❌ যা এখন নেই (Need to Implement):
Loan payment এর জন্য endpoint নেই। এই endpoint তৈরি করতে হবে:

#### Backend Endpoint (যা তৈরি করতে হবে):
```javascript
// POST /loans/:loanId/payment
// Loan এর payment record করার জন্য
```

#### Frontend এ যা করতে হবে:

```javascript
// Loan payment form component
const LoanPaymentForm = ({ loanId, onPaymentSuccess }) => {
  const [paymentData, setPaymentData] = useState({
    amount: '',
    paymentDate: new Date().toISOString().split('T')[0],
    paymentMethod: 'bank-transfer',
    targetAccountId: '', // কোন account এ money credit হবে
    notes: ''
  });

  const handlePaymentSubmit = async (e) => {
    e.preventDefault();
    
    try {
      const response = await axiosSecure.post(
        `/loans/${loanId}/payment`,
        {
          amount: parseFloat(paymentData.amount),
          paymentDate: paymentData.paymentDate,
          paymentMethod: paymentData.paymentMethod,
          targetAccountId: paymentData.targetAccountId,
          notes: paymentData.notes,
          createdBy: userProfile?.email,
          branchId: userProfile?.branchId
        }
      );
      
      if (response.data.success) {
        // Success! Payment recorded হয়েছে
        onPaymentSuccess(response.data);
      }
    } catch (error) {
      console.error('Payment error:', error);
    }
  };

  return (
    <form onSubmit={handlePaymentSubmit}>
      {/* Payment form fields */}
      <input 
        type="number" 
        name="amount" 
        placeholder="Payment Amount"
        required
      />
      <input 
        type="date" 
        name="paymentDate" 
        value={paymentData.paymentDate}
        required
      />
      <select name="targetAccountId" required>
        {/* Bank account select */}
      </select>
      <button type="submit">Record Payment</button>
    </form>
  );
};
```

---

## 3️⃣ Loan Transaction History দেখানো

### ✅ যা আছে:
- GET `/loans/:id` - Specific loan দেখার জন্য
- GET `/loans` - সব loans filter সহ

### 📝 Frontend এ যা করতে হবে:

#### Loan Details Page এ Transaction History:
```javascript
// Loan details component
const LoanDetails = ({ loanId }) => {
  const [loan, setLoan] = useState(null);
  const [transactions, setTransactions] = useState([]);

  useEffect(() => {
    // Loan data fetch
    const fetchLoan = async () => {
      const loanRes = await axiosSecure.get(`/loans/${loanId}`);
      if (loanRes.data.success) {
        setLoan(loanRes.data.loan);
      }
    };

    // Related transactions fetch
    const fetchTransactions = async () => {
      const transRes = await axiosSecure.get(`/api/transactions`, {
        params: {
          loanId: loanId // Filter by loanId
        }
      });
      if (transRes.data.success) {
        setTransactions(transRes.data.transactions || []);
      }
    };

    fetchLoan();
    fetchTransactions();
  }, [loanId]);

  return (
    <div>
      <h2>Loan Details: {loan?.loanId}</h2>
      
      {/* Loan Info */}
      <div>
        <p>Amount: {loan?.amount}</p>
        <p>Remaining: {loan?.remainingAmount}</p>
        <p>Status: {loan?.status}</p>
      </div>

      {/* Transaction History */}
      <div>
        <h3>Transaction History</h3>
        {transactions.map(tx => (
          <div key={tx._id}>
            <p>Date: {tx.date}</p>
            <p>Type: {tx.transactionType}</p>
            <p>Amount: {tx.amount}</p>
            <p>Status: {tx.status}</p>
          </div>
        ))}
      </div>
    </div>
  );
};
```

---

## 4️⃣ Loan List Page এ Transaction Info দেখানো

### 📝 Frontend Implementation:

```javascript
// Loans List Component
const LoansList = () => {
  const [loans, setLoans] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchLoans = async () => {
      try {
        const response = await axiosSecure.get('/loans', {
          params: {
            loanDirection: 'giving', // বা 'receiving'
            status: 'Active',
            page: 1,
            limit: 20
          }
        });
        
        if (response.data.success) {
          setLoans(response.data.loans);
        }
      } catch (error) {
        console.error('Error:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchLoans();
  }, []);

  // প্রতিটি loan এর জন্য transaction count fetch
  const getTransactionInfo = async (loanId) => {
    try {
      const response = await axiosSecure.get('/api/transactions', {
        params: { loanId }
      });
      return response.data.count || 0;
    } catch (error) {
      return 0;
    }
  };

  return (
    <div>
      {loans.map(loan => (
        <div key={loan._id}>
          <h3>{loan.fullName}</h3>
          <p>Loan ID: {loan.loanId}</p>
          <p>Amount: {loan.amount}</p>
          <p>Remaining: {loan.remainingAmount}</p>
          <p>Status: {loan.status}</p>
          
          {/* View Transactions Button */}
          <button onClick={() => viewTransactions(loan.loanId)}>
            View Transactions
          </button>
          
          {/* Make Payment Button (যদি giving loan হয়) */}
          {loan.loanDirection === 'giving' && loan.status === 'Active' && (
            <button onClick={() => openPaymentModal(loan)}>
              Record Payment
            </button>
          )}
        </div>
      ))}
    </div>
  );
};
```

---

## 5️⃣ Loan Payment Endpoint (Backend - যা তৈরি করতে হবে)

### 📋 Endpoint Specification:

```
POST /loans/:loanId/payment
```

#### Request Body:
```json
{
  "amount": 5000,
  "paymentDate": "2024-01-15",
  "paymentMethod": "bank-transfer",
  "targetAccountId": "account_mongodb_id",
  "notes": "Partial payment",
  "createdBy": "user@email.com",
  "branchId": "main"
}
```

#### Response:
```json
{
  "success": true,
  "message": "Payment recorded successfully",
  "payment": {
    "paymentId": "PAY123",
    "loanId": "LOANGDH2508290001",
    "amount": 5000,
    "remainingAmount": 45000,
    "transactionId": "TXN123456"
  }
}
```

#### যা করতে হবে:
1. Loan document update (remainingAmount কমাতে হবে)
2. Transaction create (credit transaction)
3. Bank account balance update (credit)
4. Payment history save

---

## 6️⃣ Loan Status Update (Approve/Reject)

### 📝 Frontend Implementation:

```javascript
// Loan approval component (Admin এর জন্য)
const LoanApproval = ({ loanId }) => {
  const handleApprove = async () => {
    try {
      const response = await axiosSecure.patch(`/loans/${loanId}/approve`, {
        targetAccountId: selectedAccountId, // কোন account এ credit হবে
        approvedBy: userProfile?.email
      });
      
      if (response.data.success) {
        // Loan approved, transaction created
      }
    } catch (error) {
      console.error('Error:', error);
    }
  };

  return (
    <button onClick={handleApprove}>
      Approve Loan
    </button>
  );
};
```

---

## 7️⃣ Complete Flow Diagram

### Loan Giving Flow:
```
1. User fills loan giving form
2. Selects bank account (targetAccountId)
3. Submits form → POST /loans/giving
4. Backend creates loan document
5. Backend creates DEBIT transaction
6. Backend updates bank account balance
7. Returns success with transactionId
```

### Loan Payment Flow:
```
1. User clicks "Make Payment" on loan
2. Opens payment form
3. Enters payment amount & selects account
4. Submits → POST /loans/:loanId/payment
5. Backend updates loan remainingAmount
6. Backend creates CREDIT transaction
7. Backend updates bank account balance
8. Returns success
```

### Loan Receiving Flow:
```
1. User fills loan application form
2. Submits → POST /loans/receiving
3. Backend creates loan (status: Pending)
4. Admin reviews and approves
5. On approval → PATCH /loans/:loanId/approve
6. Backend creates CREDIT transaction
7. Backend updates bank account balance
8. Loan status becomes Active
```

---

## 8️⃣ Important Points (মনে রাখুন)

### ✅ যা কাজ করছে:
- Loan giving এর সময় transaction automatically create হয়
- Bank account balance update হয়
- Transaction এ loan ID link থাকে

### ❌ যা তৈরি করতে হবে:
1. **Loan Payment Endpoint** - Borrower payment record করার জন্য
2. **Loan Approval Endpoint** - Loan receiving approve করার জন্য
3. **Loan Update Endpoint** - Loan details update করার জন্য

### 📝 Frontend এ যা করতে হবে:
1. Loan giving form এ bank account select field যোগ করুন
2. Loan list page তৈরি করুন
3. Loan details page তৈরি করুন (transaction history সহ)
4. Loan payment form তৈরি করুন
5. Loan approval interface তৈরি করুন (Admin এর জন্য)

---

## 9️⃣ API Endpoints Summary

### ✅ Existing Endpoints:
- `POST /loans/giving` - Loan giving (transaction auto-create)
- `POST /loans/receiving` - Loan application
- `GET /loans` - Get all loans
- `GET /loans/:id` - Get specific loan
- `GET /api/transactions?loanId=xxx` - Get transactions by loan ID

### ❌ Need to Create:
- `POST /loans/:loanId/payment` - Record loan payment
- `PATCH /loans/:loanId/approve` - Approve loan application
- `PATCH /loans/:loanId` - Update loan details
- `GET /loans/:loanId/transactions` - Get loan's transactions

---

## 🔟 Testing Checklist

### Frontend Testing:
- [ ] Loan giving form with bank account selection
- [ ] Loan list page with filters
- [ ] Loan details page with transaction history
- [ ] Loan payment form
- [ ] Loan approval interface
- [ ] Transaction history display

### Backend Testing:
- [ ] Loan payment endpoint
- [ ] Loan approval endpoint
- [ ] Loan update endpoint
- [ ] Transaction creation on loan events
- [ ] Bank account balance updates

---

**Note**: এই guide follow করলে loan system সম্পূর্ণ transaction track করতে পারবে! 🎯

