// Load environment variables
require("dotenv").config();

const express = require("express");
const http = require("http");
const cors = require("cors");
const jwt = require("jsonwebtoken");
const { MongoClient, ObjectId, ServerApiVersion } = require("mongodb");

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

// MongoDB Setup
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
let db, users, branches, counters, customers, customerTypes, transactions, services, sales, vendors, orders, bankAccounts, categories, agents;

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
    transactions = db.collection("transactions");
    services = db.collection("services");
    sales = db.collection("sales");
    vendors = db.collection("vendors");
    orders = db.collection("orders");
    bankAccounts = db.collection("bankAccounts");
    categories = db.collection("categories");
    agents = db.collection("agents");

    


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
          postCode
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
          issueDate: issueDate || null,
          expiryDate: expiryDate || null,
          dateOfBirth: dateOfBirth || null,
          nidNumber: nidNumber || null,
          // Additional fields
          notes: notes || null,
          referenceBy: referenceBy || null,
          referenceCustomerId: referenceCustomerId || null,
          createdAt: new Date(),
          updatedAt: new Date(),
          isActive: true
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
            passportNumber: newCustomer.passportNumber,
            issueDate: newCustomer.issueDate,
            expiryDate: newCustomer.expiryDate,
            dateOfBirth: newCustomer.dateOfBirth,
            nidNumber: newCustomer.nidNumber,
            notes: newCustomer.notes,
            referenceBy: newCustomer.referenceBy,
            referenceCustomerId: newCustomer.referenceCustomerId,
            customerImage: newCustomer.customerImage,
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

        const { customerType, division, district, upazila, search, passportNumber, nidNumber, expiringSoon } = req.query;
        
        let filter = { isActive: true };
        
        // Apply filters
        if (customerType) filter.customerType = customerType;
        if (division) filter.division = division;
        if (district) filter.district = district;
        if (upazila) filter.upazila = upazila;
        if (passportNumber) filter.passportNumber = { $regex: passportNumber, $options: 'i' };
        if (nidNumber) filter.nidNumber = { $regex: nidNumber, $options: 'i' };
        
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

    // Get customer by ID
    app.get("/customers/:customerId", async (req, res) => {
      try {
        const { customerId } = req.params;
        
        const customer = await customers.findOne({ 
          customerId: customerId,
          isActive: true 
        });

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
        const updateData = req.body;
        
        // Remove fields that shouldn't be updated
        delete updateData.customerId;
        delete updateData.createdAt;
        updateData.updatedAt = new Date();

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

        res.send({
          success: true,
          message: "Customer updated successfully",
          modifiedCount: result.modifiedCount
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



 

// ==================== TRANSACTION ROUTES ====================

// Create new transaction
app.post("/transactions", async (req, res) => {
  try {
    const {
      transactionType,
      customerId,
      category,
      paymentMethod,
      paymentDetails,
      customerBankAccount,
      notes,
      date,
      createdBy,
      branchId
    } = req.body;

    // Validation
    if (!transactionType || !customerId || !category || !paymentMethod || !paymentDetails || !date) {
      return res.status(400).json({
        error: true,
        message: "Transaction type, customer ID, category, payment method, payment details, and date are required"
      });
    }

    // Validate transaction type
    if (!['credit', 'debit'].includes(transactionType)) {
      return res.status(400).json({
        error: true,
        message: "Transaction type must be 'credit' or 'debit'"
      });
    }

    // Validate and normalize payment method (frontend ids)
    const validPaymentMethods = ['cash', 'bank-transfer', 'cheque', 'mobile-banking', 'others', 'bank'];
    if (!validPaymentMethods.includes(paymentMethod)) {
      return res.status(400).json({
        error: true,
        message: "Invalid payment method"
      });
    }

    // Normalize to canonical storage values
    const normalizedPaymentMethod = paymentMethod === 'bank' ? 'bank-transfer' : paymentMethod;

    // Validate date format
    if (!isValidDate(date)) {
      return res.status(400).json({
        error: true,
        message: "Invalid date format. Please use YYYY-MM-DD format"
      });
    }

    // Validate amount
    const parsedAmount = parseFloat(paymentDetails?.amount);
    if (!parsedAmount || parsedAmount <= 0) {
      return res.status(400).json({
        error: true,
        message: "Amount must be greater than 0"
      });
    }

    // Check if customer exists
    const customer = await customers.findOne({ 
      customerId: customerId,
      isActive: true 
    });

    if (!customer) {
      return res.status(404).json({
        error: true,
        message: "Customer not found"
      });
    }

    // Get branch information
    const branch = await branches.findOne({ branchId, isActive: true });
    if (!branch) {
      return res.status(400).json({
        error: true,
        message: "Invalid branch ID"
      });
    }

    // Generate unique transaction ID
    const transactionId = await generateTransactionId(db, branch.branchCode);

    // Create transaction object
    const newTransaction = {
      transactionId,
      transactionType,
      customerId: customer.customerId,
      customerName: customer.name,
      customerPhone: customer.mobile,
      customerEmail: customer.email,
      category,
      paymentMethod: normalizedPaymentMethod,
      paymentDetails: {
        bankName: paymentDetails?.bankName || null,
        accountNumber: paymentDetails?.accountNumber || null,
        chequeNumber: paymentDetails?.chequeNumber || null,
        mobileProvider: paymentDetails?.mobileProvider || null,
        transactionId: paymentDetails?.transactionId || null,
        amount: parsedAmount,
        reference: paymentDetails?.reference || null
      },
      customerBankAccount: {
        bankName: customerBankAccount?.bankName || null,
        accountNumber: customerBankAccount?.accountNumber || null
      },
      notes: notes || null,
      date: new Date(date),
      createdBy: createdBy || null,
      branchId: branch.branchId,
      branchName: branch.branchName,
      branchCode: branch.branchCode,
      status: 'completed',
      createdAt: new Date(),
      updatedAt: new Date(),
      isActive: true
    };

    const result = await transactions.insertOne(newTransaction);

    res.status(201).json({
      success: true,
      message: "Transaction created successfully",
      transaction: {
        _id: result.insertedId,
        transactionId: newTransaction.transactionId,
        transactionType: newTransaction.transactionType,
        customerId: newTransaction.customerId,
        customerName: newTransaction.customerName,
        customerPhone: newTransaction.customerPhone,
        customerEmail: newTransaction.customerEmail,
        category: newTransaction.category,
        paymentMethod: newTransaction.paymentMethod,
        paymentDetails: newTransaction.paymentDetails,
        notes: newTransaction.notes,
        date: newTransaction.date,
        branchId: newTransaction.branchId,
        branchName: newTransaction.branchName,
        status: newTransaction.status,
        createdAt: newTransaction.createdAt
      }
    });

  } catch (error) {
    console.error('Create transaction error:', error);
    res.status(500).json({ 
      error: true, 
      message: "Internal server error while creating transaction" 
    });
  }
});
    

    // Get all transactions with filters
    app.get("/transactions", async (req, res) => {
      try {
        const { 
          transactionType, 
          category, 
          paymentMethod, 
          branchId, 
          customerId, 
          dateFrom, 
          dateTo, 
          search,
          status,
          page = 1,
          limit = 20
        } = req.query;
        
        let filter = { isActive: true };
        
        // Apply filters
        if (transactionType) filter.transactionType = transactionType;
        if (category) filter.category = category;
        if (paymentMethod) filter.paymentMethod = paymentMethod;
        if (branchId) filter.branchId = branchId;
        if (customerId) filter.customerId = customerId;
        if (status) filter.status = status;
        
        // Date range filter
        if (dateFrom || dateTo) {
          filter.date = {};
          if (dateFrom) filter.date.$gte = new Date(dateFrom);
          if (dateTo) {
            const endDate = new Date(dateTo);
            endDate.setHours(23, 59, 59, 999);
            filter.date.$lte = endDate;
          }
        }
        
        // Search filter
        if (search) {
          filter.$or = [
            { transactionId: { $regex: search, $options: 'i' } },
            { customerName: { $regex: search, $options: 'i' } },
            { customerPhone: { $regex: search, $options: 'i' } },
            { customerEmail: { $regex: search, $options: 'i' } },
            { notes: { $regex: search, $options: 'i' } }
          ];
        }

        // Calculate pagination
        const skip = (parseInt(page) - 1) * parseInt(limit);
        
        // Get total count
        const totalCount = await transactions.countDocuments(filter);
        
        // Get transactions with pagination
        const allTransactions = await transactions.find(filter)
          .sort({ createdAt: -1 })
          .skip(skip)
          .limit(parseInt(limit))
          .toArray();

        res.json({
          success: true,
          count: allTransactions.length,
          totalCount,
          currentPage: parseInt(page),
          totalPages: Math.ceil(totalCount / parseInt(limit)),
          transactions: allTransactions
        });
      } catch (error) {
        console.error('Get transactions error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while fetching transactions" 
        });
      }
    });

    // Get transaction by ID
    app.get("/transactions/:transactionId", async (req, res) => {
      try {
        const { transactionId } = req.params;
        
        const transaction = await transactions.findOne({ 
          transactionId: transactionId,
          isActive: true 
        });

        if (!transaction) {
          return res.status(404).json({ 
            error: true, 
            message: "Transaction not found" 
          });
        }

        res.json({
          success: true,
          transaction: transaction
        });
      } catch (error) {
        console.error('Get transaction error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while fetching transaction" 
        });
      }
    });

    // Update transaction
    app.patch("/transactions/:transactionId", async (req, res) => {
      try {
        const { transactionId } = req.params;
        const updateData = req.body;
        
        // Remove fields that shouldn't be updated
        delete updateData.transactionId;
        delete updateData.createdAt;
        updateData.updatedAt = new Date();

        // Validate transaction type if being updated
        if (updateData.transactionType && !['credit', 'debit'].includes(updateData.transactionType)) {
          return res.status(400).json({
            error: true,
            message: "Transaction type must be 'credit' or 'debit'"
          });
        }

        // Validate payment method if being updated
        if (updateData.paymentMethod) {
          const validPaymentMethods = ['cash', 'bank-transfer', 'cheque', 'mobile-banking', 'others', 'bank'];
          if (!validPaymentMethods.includes(updateData.paymentMethod)) {
            return res.status(400).json({
              error: true,
              message: "Invalid payment method"
            });
          }
          // Normalize legacy 'bank' to 'bank-transfer'
          if (updateData.paymentMethod === 'bank') {
            updateData.paymentMethod = 'bank-transfer';
          }
        }

        // Validate date if being updated
        if (updateData.date && !isValidDate(updateData.date)) {
          return res.status(400).json({
            error: true,
            message: "Invalid date format. Please use YYYY-MM-DD format"
          });
        }

        const result = await transactions.updateOne(
          { transactionId: transactionId, isActive: true },
          { $set: updateData }
        );

        if (result.matchedCount === 0) {
          return res.status(404).json({ 
            error: true, 
            message: "Transaction not found" 
          });
        }

        res.json({
          success: true,
          message: "Transaction updated successfully",
          modifiedCount: result.modifiedCount
        });
      } catch (error) {
        console.error('Update transaction error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while updating transaction" 
        });
      }
    });

    // Delete transaction (soft delete)
    app.delete("/transactions/:transactionId", async (req, res) => {
      try {
        const { transactionId } = req.params;
        
        const result = await transactions.updateOne(
          { transactionId: transactionId, isActive: true },
          { $set: { isActive: false, updatedAt: new Date() } }
        );

        if (result.matchedCount === 0) {
          return res.status(404).json({ 
            error: true, 
            message: "Transaction not found" 
          });
        }

        res.json({
          success: true,
          message: "Transaction deleted successfully"
        });
      } catch (error) {
        console.error('Delete transaction error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while deleting transaction" 
        });
      }
    });

    // Get transaction statistics
    app.get("/transactions/stats/overview", async (req, res) => {
      try {
        const { branchId, dateFrom, dateTo } = req.query;
        
        let filter = { isActive: true };
        
        // Apply branch filter
        if (branchId) filter.branchId = branchId;
        
        // Apply date range filter
        if (dateFrom || dateTo) {
          filter.date = {};
          if (dateFrom) filter.date.$gte = new Date(dateFrom);
          if (dateTo) {
            const endDate = new Date(dateTo);
            endDate.setHours(23, 59, 59, 999);
            filter.date.$lte = endDate;
          }
        }

        // Get total transactions
        const totalTransactions = await transactions.countDocuments(filter);
        
        // Get credit transactions
        const creditTransactions = await transactions.countDocuments({
          ...filter,
          transactionType: 'credit'
        });
        
        // Get debit transactions
        const debitTransactions = await transactions.countDocuments({
          ...filter,
          transactionType: 'debit'
        });

        // Get total amounts
        const creditAmount = await transactions.aggregate([
          { $match: { ...filter, transactionType: 'credit' } },
          { $group: { _id: null, total: { $sum: '$paymentDetails.amount' } } }
        ]).toArray();

        const debitAmount = await transactions.aggregate([
          { $match: { ...filter, transactionType: 'debit' } },
          { $group: { _id: null, total: { $sum: '$paymentDetails.amount' } } }
        ]).toArray();

        // Get today's transactions
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const todayEnd = new Date();
        todayEnd.setHours(23, 59, 59, 999);
        
        const todayTransactions = await transactions.countDocuments({
          ...filter,
          createdAt: { $gte: today, $lte: todayEnd }
        });

        // Get this month's transactions
        const thisMonth = new Date();
        thisMonth.setDate(1);
        thisMonth.setHours(0, 0, 0, 0);
        
        const thisMonthTransactions = await transactions.countDocuments({
          ...filter,
          createdAt: { $gte: thisMonth }
        });

        // Get transactions by category
        const transactionsByCategory = await transactions.aggregate([
          { $match: filter },
          { $group: { _id: '$category', count: { $sum: 1 }, totalAmount: { $sum: '$paymentDetails.amount' } } },
          { $sort: { count: -1 } }
        ]).toArray();

        // Get transactions by payment method
        const transactionsByPaymentMethod = await transactions.aggregate([
          { $match: filter },
          { $group: { _id: '$paymentMethod', count: { $sum: 1 }, totalAmount: { $sum: '$paymentDetails.amount' } } },
          { $sort: { count: -1 } }
        ]).toArray();

        res.json({
          success: true,
          stats: {
            total: totalTransactions,
            credit: creditTransactions,
            debit: debitTransactions,
            creditAmount: creditAmount[0]?.total || 0,
            debitAmount: debitAmount[0]?.total || 0,
            netAmount: (creditAmount[0]?.total || 0) - (debitAmount[0]?.total || 0),
            today: todayTransactions,
            thisMonth: thisMonthTransactions,
            byCategory: transactionsByCategory,
            byPaymentMethod: transactionsByPaymentMethod
          }
        });
      } catch (error) {
        console.error('Get transaction stats error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while fetching transaction statistics" 
        });
      }
    });

    // Get transaction report by date range
    app.get("/transactions/report", async (req, res) => {
      try {
        const { 
          dateFrom, 
          dateTo, 
          branchId, 
          transactionType,
          category,
          paymentMethod,
          format = 'json'
        } = req.query;

        if (!dateFrom || !dateTo) {
          return res.status(400).json({
            error: true,
            message: "Date range (dateFrom and dateTo) is required"
          });
        }

        let filter = { isActive: true };
        
        // Apply filters
        if (branchId) filter.branchId = branchId;
        if (transactionType) filter.transactionType = transactionType;
        if (category) filter.category = category;
        if (paymentMethod) filter.paymentMethod = paymentMethod;
        
        // Date range filter
        const startDate = new Date(dateFrom);
        const endDate = new Date(dateTo);
        endDate.setHours(23, 59, 59, 999);
        filter.date = { $gte: startDate, $lte: endDate };

        // Get transactions
        const reportTransactions = await transactions.find(filter)
          .sort({ date: -1 })
          .toArray();

        // Calculate summary
        const summary = {
          totalTransactions: reportTransactions.length,
          totalCreditAmount: 0,
          totalDebitAmount: 0,
          creditCount: 0,
          debitCount: 0
        };

        reportTransactions.forEach(transaction => {
          if (transaction.transactionType === 'credit') {
            summary.totalCreditAmount += transaction.paymentDetails.amount;
            summary.creditCount++;
          } else {
            summary.totalDebitAmount += transaction.paymentDetails.amount;
            summary.debitCount++;
          }
        });

        summary.netAmount = summary.totalCreditAmount - summary.totalDebitAmount;

        res.json({
          success: true,
          report: {
            dateRange: {
              from: dateFrom,
              to: dateTo
            },
            summary,
            transactions: reportTransactions
          }
        });
      } catch (error) {
        console.error('Get transaction report error:', error);
        res.status(500).json({ 
          error: true, 
          message: "Internal server error while generating transaction report" 
        });
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

// ✅ DELETE (soft delete)
// app.delete("/vendors/:id", async (req, res) => {
//   try {
//     const { id } = req.params;

//     if (!ObjectId.isValid(id)) {
//       return res.status(400).json({ error: true, message: "Invalid vendor ID" });
//     }

//     const result = await vendors.updateOne(
//       { _id: new ObjectId(id) },
//       { $set: { isActive: false } }
//     );

//     if (result.modifiedCount === 0) {
//       return res.status(404).json({ error: true, message: "Vendor not found" });
//     }

//     res.json({ success: true, message: "Vendor deleted successfully" });
//   } catch (error) {
//     console.error("Error deleting vendor:", error);
//     res.status(500).json({
//       error: true,
//       message: "Internal server error while deleting vendor",
//     });
//   }
// });




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
                { $and: [
                  { $or: [{ $eq: ["$nid", ""] }, { $eq: ["$nid", null] }] },
                  { $or: [{ $eq: ["$passport", ""] }, { $eq: ["$passport", null] }] }
                ]}, 
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

    // Check if vendor exists - handle both MongoDB ObjectId and string ID
    let vendor;
    if (ObjectId.isValid(vendorId)) {
      vendor = await vendors.findOne({ 
        _id: new ObjectId(vendorId),
        isActive: true 
      });
    } else {
      // If not a valid ObjectId, search by other fields
      vendor = await vendors.findOne({ 
        $or: [
          { _id: vendorId },
          { tradeName: vendorId }
        ],
        isActive: true 
      });
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
                { $and: [
                  { $ne: ["$nid", ""] },
                  { $ne: ["$passport", ""] }
                ]}, 
                1, 
                0
              ] 
            } 
          },
          withoutDocs: { 
            $sum: { 
              $cond: [
                { $and: [
                  { $or: [{ $eq: ["$nid", ""] }, { $eq: ["$nid", null] }] },
                  { $or: [{ $eq: ["$passport", ""] }, { $eq: ["$passport", null] }] }
                ]}, 
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
app.post("/haj-umrah/agents", async (req, res) => {
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
app.get("/haj-umrah/agents", async (req, res) => {
  try {
    const { page = 1, limit = 10, q } = req.query;
    const pageNum = Math.max(parseInt(page) || 1, 1);
    const limitNum = Math.min(Math.max(parseInt(limit) || 10, 1), 100);

    const filter = { };
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
app.get("/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).send({ error: true, message: "Invalid agent id" });
    }
    const agent = await agents.findOne({ _id: new ObjectId(id) });
    if (!agent) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }
    res.send({ success: true, data: agent });
  } catch (error) {
    console.error('Get agent error:', error);
    res.status(500).json({ error: true, message: "Internal server error while fetching agent" });
  }
});

// Update agent
app.put("/haj-umrah/agents/:id", async (req, res) => {
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
      isActive
    } = req.body;

    const update = { $set: { updatedAt: new Date() } };
    if (tradeName !== undefined) update.$set.tradeName = String(tradeName).trim();
    if (tradeLocation !== undefined) update.$set.tradeLocation = String(tradeLocation).trim();
    if (ownerName !== undefined) update.$set.ownerName = String(ownerName).trim();
    if (contactNo !== undefined) update.$set.contactNo = String(contactNo).trim();
    if (dob !== undefined) {
      if (dob && !isValidDate(dob)) {
        return res.status(400).send({ error: true, message: "Invalid date format for dob (YYYY-MM-DD)" });
      }
      update.$set.dob = dob || null;
    }
    if (nid !== undefined) update.$set.nid = nid || "";
    if (passport !== undefined) update.$set.passport = passport || "";
    if (isActive !== undefined) update.$set.isActive = Boolean(isActive);

    // Validate fields if provided
    if (update.$set.contactNo) {
      const phoneRegex = /^\+?[0-9\-()\s]{6,20}$/;
      if (!phoneRegex.test(update.$set.contactNo)) {
        return res.status(400).send({ error: true, message: "Enter a valid phone number" });
      }
    }
    if (update.$set.nid && !/^[0-9]{8,20}$/.test(update.$set.nid)) {
      return res.status(400).send({ error: true, message: "NID should be 8-20 digits" });
    }
    if (update.$set.passport && !/^[A-Za-z0-9]{6,12}$/.test(update.$set.passport)) {
      return res.status(400).send({ error: true, message: "Passport should be 6-12 chars" });
    }

    const result = await agents.updateOne({ _id: new ObjectId(id) }, update);
    if (result.matchedCount === 0) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }

    const updated = await agents.findOne({ _id: new ObjectId(id) });
    res.send({ success: true, message: "Agent updated successfully", data: updated });
  } catch (error) {
    console.error('Update agent error:', error);
    res.status(500).json({ error: true, message: "Internal server error while updating agent" });
  }
});

// Delete agent (hard delete)
app.delete("/haj-umrah/agents/:id", async (req, res) => {
  try {
    const { id } = req.params;
    if (!ObjectId.isValid(id)) {
      return res.status(400).send({ error: true, message: "Invalid agent id" });
    }
    const result = await agents.deleteOne({ _id: new ObjectId(id) });
    if (result.deletedCount === 0) {
      return res.status(404).send({ error: true, message: "Agent not found" });
    }
    res.send({ success: true, message: "Agent deleted successfully" });
  } catch (error) {
    console.error('Delete agent error:', error);
    res.status(500).json({ error: true, message: "Internal server error while deleting agent" });
  }
});

// ==================== BANK ACCOUNTS ROUTES ====================
// Schema (MongoDB):
// {
//   bankName, accountNumber, accountType, branchName, accountHolder,
//   initialBalance, currentBalance, currency, contactNumber,
//   status: 'Active'|'Inactive', createdAt, updatedAt, isDeleted, balanceHistory?
// }

// Create bank account
app.post("/bank-accounts", async (req, res) => {
  try {
    const {
      bankName,
      accountNumber,
      accountType = "Current",
      branchName,
      accountHolder,
      initialBalance,
      currency = "BDT",
      contactNumber
    } = req.body || {};

    if (!bankName || !accountNumber || !accountType || !branchName || !accountHolder || initialBalance === undefined) {
      return res.status(400).json({ success: false, error: "Missing required fields" });
    }

    const numericInitial = Number(initialBalance);
    if (!Number.isFinite(numericInitial) || numericInitial < 0) {
      return res.status(400).json({ success: false, error: "Invalid initialBalance" });
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
      branchName,
      accountHolder,
      initialBalance: numericInitial,
      currentBalance: numericInitial,
      currency,
      contactNumber: contactNumber || null,
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
    const { status, accountType, currency, search } = req.query || {};
    const query = { isDeleted: { $ne: true } };
    if (status) query.status = status;
    if (accountType) query.accountType = accountType;
    if (currency) query.currency = currency;
    if (search) {
      query.$or = [
        { bankName: { $regex: search, $options: "i" } },
        { accountNumber: { $regex: search, $options: "i" } },
        { branchName: { $regex: search, $options: "i" } },
        { accountHolder: { $regex: search, $options: "i" } }
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
app.patch("/bank-accounts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const update = { ...req.body };

    if (update.initialBalance !== undefined) {
      const numeric = Number(update.initialBalance);
      if (!Number.isFinite(numeric) || numeric < 0) {
        return res.status(400).json({ success: false, error: "Invalid initialBalance" });
      }
      update.initialBalance = numeric;
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
    if (!result || !result.value) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }
    res.json({ success: true, data: result.value });
  } catch (error) {
    console.error("❌ Error updating bank account:", error);
    res.status(500).json({ success: false, error: "Failed to update bank account" });
  }
});

// Soft delete bank account
app.delete("/bank-accounts/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id), isDeleted: { $ne: true } },
      { $set: { isDeleted: true, status: "Inactive", updatedAt: new Date() } },
      { returnDocument: "after" }
    );
    if (!result || !result.value) {
      return res.status(404).json({ success: false, error: "Bank account not found" });
    }
    res.json({ success: true, data: result.value });
  } catch (error) {
    console.error("❌ Error deleting bank account:", error);
    res.status(500).json({ success: false, error: "Failed to delete bank account" });
  }
});

// Balance adjustment
app.post("/bank-accounts/:id/adjust-balance", async (req, res) => {
  try {
    const { id } = req.params;
    const { amount, type, note } = req.body || {};

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

    const update = {
      currentBalance: newBalance,
      updatedAt: new Date()
    };

    const result = await bankAccounts.findOneAndUpdate(
      { _id: new ObjectId(id) },
      { $set: update, $push: { balanceHistory: { amount: numericAmount, type, note: note || null, at: new Date() } } },
      { returnDocument: "after" }
    );
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
          activeAccounts: { $sum: { $cond: [{ $eq: ["$status", "Active"] }, 1, 0] } }
        }
      }
    ];
    const stats = await bankAccounts.aggregate(pipeline).toArray();
    const data = stats[0] || { totalAccounts: 0, totalBalance: 0, totalInitialBalance: 0, activeAccounts: 0 };
    res.json({ success: true, data });
  } catch (error) {
    console.error("❌ Error getting bank stats:", error);
    res.status(500).json({ success: false, error: "Failed to get bank stats" });
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
