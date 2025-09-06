// Load environment variables
require("dotenv").config();

const express = require("express");
const cors = require("cors");
const jwt = require("jsonwebtoken");
const { MongoClient, ObjectId, ServerApiVersion } = require("mongodb");

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(cors({
  origin: 'http://localhost:5173', // Vite dev server
  credentials: true
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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
  
  console.log("✅ Default branches initialized successfully");
};

// Main async function to run server logic
async function run() {
  try {
    await client.connect();
    console.log("✅ MongoDB connected");

    const db = client.db("erpDashboard");
    const users = db.collection("users");
    const branches = db.collection("branches");
    const counters = db.collection("counters");
    const customers = db.collection("customers");
    const customerTypes = db.collection("customerTypes");
    const transactions = db.collection("transactions");

    // Initialize default branches
    await initializeDefaultBranches(db, branches, counters);
    
    // Initialize default customer types
    await initializeDefaultCustomerTypes(db, customerTypes);

    // ==================== ROOT ENDPOINT ====================
    app.get("/", (req, res) => {
      res.send("🚀 ERP Dashboard API is running!");
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
        
        // Remove fields that shouldn't be updated
        delete updateData._id;
        delete updateData.createdAt;
        updateData.updatedAt = new Date();

        // Check if value is being updated and if it already exists
        if (updateData.value) {
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
        
        // Check if any customers are using this type
        const customersUsingType = await customers.countDocuments({ 
          customerType: req.body.value || (await customerTypes.findOne({ _id: new ObjectId(id) }))?.value,
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

        if (result.matchedCount === 0) {
          return res.status(404).json({ 
            error: true, 
            message: "Customer type not found" 
          });
        }

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

        // Validate payment method
        const validPaymentMethods = ['bank', 'cheque', 'mobile-banking'];
        if (!validPaymentMethods.includes(paymentMethod)) {
          return res.status(400).json({
            error: true,
            message: "Invalid payment method"
          });
        }

        // Validate date format
        if (!isValidDate(date)) {
          return res.status(400).json({
            error: true,
            message: "Invalid date format. Please use YYYY-MM-DD format"
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
          paymentMethod,
          paymentDetails: {
            bankName: paymentDetails.bankName || null,
            accountNumber: paymentDetails.accountNumber || null,
            chequeNumber: paymentDetails.chequeNumber || null,
            mobileProvider: paymentDetails.mobileProvider || null,
            transactionId: paymentDetails.transactionId || null,
            amount: parseFloat(paymentDetails.amount) || 0,
            reference: paymentDetails.reference || null
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
          const validPaymentMethods = ['bank', 'cheque', 'mobile-banking'];
          if (!validPaymentMethods.includes(updateData.paymentMethod)) {
            return res.status(400).json({
              error: true,
              message: "Invalid payment method"
            });
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

    // Start server
    app.listen(port, () => {
      console.log(`🚀 Server is running on port ${port}`);
    });

  } catch (error) {
    console.error("❌ Server startup error:", error);
    process.exit(1);
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n🛑 Shutting down gracefully...');
  await client.close();
  process.exit(0);
});

// Run the server
run().catch(console.error);
