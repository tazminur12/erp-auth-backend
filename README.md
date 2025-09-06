# ERP Dashboard Backend API

একটি সম্পূর্ণ ERP Dashboard Backend API সিস্টেম Node.js, Express, এবং MongoDB (Mongoose) দিয়ে তৈরি।

## 🚀 Features

- **User Management**: Complete CRUD operations for users
- **Branch Management**: Multi-branch support with unique ID generation
- **Role-based Access Control**: 5 different user roles
- **Auto Unique ID Generation**: Branch-wise sequential IDs (e.g., DH-0001, BOG-0001)
- **JWT Authentication**: Secure token-based authentication
- **MongoDB Integration**: Using Mongoose ODM

## 🛠️ Tech Stack

- **Runtime**: Node.js
- **Framework**: Express.js
- **Database**: MongoDB + Native Driver
- **Authentication**: JWT (JSON Web Tokens)
- **Middleware**: cors, dotenv

## 📁 Project Structure

```
erp-auth-backend/
├── index.js          # Main server file with MongoDB native driver
├── package.json      # Dependencies and scripts
└── .env              # Environment variables (create this)
```

**Note**: This project uses MongoDB native driver instead of Mongoose for better performance and control.

## 🔧 Installation & Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **MongoDB Atlas Setup:**
   - Go to [MongoDB Atlas](https://cloud.mongodb.com/)
   - Create/Select your cluster
   - Go to Database Access → Add New Database User
   - Create username and password
   - Go to Network Access → Add IP Address (0.0.0.0/0 for all IPs)
   - Get your connection string

3. **Create .env file:**
   ```env
   PORT=3000
   JWT_SECRET=your_super_secret_key_here
   JWT_EXPIRES=7d
   DB_USER=your_mongodb_atlas_username
   DB_PASSWORD=your_mongodb_atlas_password
   ```

4. **Update MongoDB URI in index.js:**
   ```javascript
   const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASSWORD}@YOUR_CLUSTER_URL/?retryWrites=true&w=majority&appName=YOUR_APP_NAME`;
   ```

5. **Run the server:**
   ```bash
   # Development mode
   npm run dev
   
   # Production mode
   npm start
   ```

## 🗄️ Database Models

### User Model
- `uniqueId`: Auto-generated unique ID (e.g., "DH-0001")
- `displayName`: User's display name
- `email`: Unique email address
- `branchId`: Branch identifier
- `branchName`: Branch name
- `branchLocation`: Branch location
- `firebaseUid`: Firebase user ID
- `role`: User role (superadmin, admin, account, reservation, user)
- `isActive`: Account status
- `createdAt`, `updatedAt`: Timestamps

### Branch Model
- `branchId`: Unique branch identifier
- `branchName`: Branch name
- `branchLocation`: Branch location
- `branchCode`: Branch code (e.g., "DH", "BOG")
- `isActive`: Branch status
- `createdAt`, `updatedAt`: Timestamps

### Counter Model
- `branchCode`: Branch code
- `sequence`: Current sequence number for unique ID generation

## 🔐 User Roles

1. **superadmin**: Full system access
2. **admin**: User and branch management
3. **account**: Account-related operations
4. **reservation**: Reservation operations
5. **user**: Basic user access

## 🌿 Default Branches

The system automatically creates these branches on startup:
- **main** → "DH" (Dhaka)
- **bogra** → "BOG" (Bogra)
- **dupchanchia** → "DUP" (Dupchanchia)
- **chittagong** → "CTG" (Chittagong)
- **sylhet** → "SYL" (Sylhet)
- **rajshahi** → "RAJ" (Rajshahi)
- **khulna** → "KHU" (Khulna)
- **barisal** → "BAR" (Barisal)
- **rangpur** → "RAN" (Rangpur)
- **mymensingh** → "MYM" (Mymensingh)

## 📡 API Endpoints

### Root & Dashboard
- `GET /` - Simple text message: "🚀 ERP Dashboard API is running!"
- `GET /dashboard` - Complete system dashboard with stats, users, branches

### Authentication
- `POST /api/auth/login` - User login/signup (auto-creates new users)
- `GET /api/auth/me` - Get current user profile

### User Management
- `POST /api/users` - Create user (admin/superadmin only)
- `GET /api/users` - List users (admin/superadmin only)
- `GET /api/users/:id` - Get user profile (protected)
- `PUT /api/users/:id` - Update user (admin/superadmin or owner)
- `DELETE /api/users/:id` - Soft delete user (admin/superadmin only)

### Branch Management
- `GET /api/branches` - List all branches
- `GET /api/branches/active` - Get active branches for signup
- `GET /api/branches/:id` - Get specific branch
- `POST /api/branches` - Add new branch (admin/superadmin only)
- `PUT /api/branches/:id` - Update branch (admin/superadmin only)
- `DELETE /api/branches/:id` - Soft delete branch (admin/superadmin only)
- `GET /api/branches/:id/users` - Get users in a branch (admin/superadmin only)

## 🔑 Authentication

### Login/Signup Request
```json
POST /api/auth/login
{
  "email": "user@example.com",
  "firebaseUid": "firebase_user_id_here",
  "displayName": "User Name",     // Required for new users
  "branchId": "main"              // Required for new users
}
```

**Note**: This endpoint handles both login and signup:
- If user exists → Login
- If user doesn't exist → Auto signup (requires displayName and branchId)

### Response
```json
{
  "message": "Signup successful" | "Login successful",
  "token": "jwt_token_here",
  "user": {
    "uniqueId": "DH-0001",
    "displayName": "John Doe",
    "email": "user@example.com",
    "role": "user",
    "branchId": "main",
    "branchName": "Main Office",
    "branchLocation": "Dhaka, Bangladesh",
    "isNewUser": true | false
  }
}
```

### Using JWT Token
Include the token in the Authorization header:
```
Authorization: Bearer your_jwt_token_here
```

## 🆔 Unique ID Generation

The system automatically generates unique IDs for users:
- Format: `[BRANCH_CODE]-[4_DIGIT_NUMBER]`
- Examples: `DH-0001`, `DH-0002`, `BOG-0001`, `BOG-0002`
- Each branch maintains its own counter
- IDs are generated atomically to prevent duplicates

## 📝 Example Usage

### Login/Signup (Auto-creates new users)
```bash
# New user signup
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "jane@example.com",
    "firebaseUid": "firebase_uid_here",
    "displayName": "Jane Doe",
    "branchId": "main"
  }'

# Existing user login
curl -X POST http://localhost:3000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "jane@example.com",
    "firebaseUid": "firebase_uid_here"
  }'
```

### Get Available Branches for Signup
```bash
curl -X GET "http://localhost:3000/api/branches/active"
```



### List Users (Admin only)
```bash
curl -X GET "http://localhost:3000/api/users?page=1&limit=10" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### Get Branches
```bash
curl -X GET "http://localhost:3000/api/branches"
```

## 🚨 Error Handling

The API returns consistent error responses:
```json
{
  "error": "Error message",
  "message": "Detailed error description"
}
```

Common HTTP status codes:
- `200` - Success
- `201` - Created
- `400` - Bad Request
- `401` - Unauthorized
- `403` - Forbidden
- `404` - Not Found
- `500` - Internal Server Error

## 🔒 Security Features

- JWT token-based authentication
- Role-based access control
- Input validation and sanitization
- Soft delete for data integrity
- Branch isolation for users
- Protected admin endpoints

## 🚀 Running the Server

```bash
# Development (with auto-reload)
npm run dev

# Production
npm start

# Root (Simple status)
curl http://localhost:3000/

# Dashboard (Complete info)
curl http://localhost:3000/dashboard
```

## 📊 Database Connection

The server automatically connects to MongoDB and initializes default branches on startup. Make sure MongoDB is running on the specified URI.

## 🎯 Key Features Summary

### ✅ What's Working
- **Auto Login/Signup**: Single endpoint handles both login and signup
- **Branch-based Unique IDs**: DH-0001, BOG-0001, CTG-0001 format
- **5 User Roles**: superadmin, admin, account, reservation, user
- **JWT Authentication**: Secure token-based auth
- **MongoDB Native Driver**: Better performance and control
- **Role-based Access**: Protected admin endpoints
- **Auto Branch Initialization**: 10 default branches on startup

### 🔄 How It Works
1. **Frontend** calls `/api/branches/active` to get available branches
2. **User selects branch** and provides email, firebaseUid, displayName
3. **Backend** automatically generates unique ID (e.g., DH-0001)
4. **JWT token** is returned for authentication
5. **User data** is stored in MongoDB with branch information

### 🚀 Ready to Use
This is a complete backend system ready for production use. All functionality is contained in a single `index.js` file using MongoDB native driver for easy deployment and maintenance.

### 🔧 Troubleshooting
If you get MongoDB authentication error:
1. Check your MongoDB Atlas credentials
2. Ensure Network Access allows your IP
3. Verify database user has proper permissions
4. Check connection string format in index.js
