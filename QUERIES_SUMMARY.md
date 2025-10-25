# Frontend Queries Summary

## ✅ useAgentQueries.js - Agent Management

### Hooks:
1. **useAgents(page, limit, searchTerm)** - Get all agents with pagination
   - Endpoint: `/api/haj-umrah/agents`
   - Returns: agents list with pagination

2. **useAgent(id)** - Get single agent with packages
   - Endpoint: `/api/haj-umrah/agents/:id`
   - Returns: agent details + packages array

3. **useCreateAgent()** - Create new agent
   - Endpoint: `/haj-umrah/agents` (POST)
   - Auto initializes due amounts to 0

4. **useUpdateAgent()** - Update agent
   - Endpoint: `/haj-umrah/agents/:id` (PUT)
   - Can update due amounts

5. **useDeleteAgent()** - Delete agent
   - Endpoint: `/haj-umrah/agents/:id` (DELETE)

---

## ✅ useAgentPackageQueries.js - Agent Package Management

### Hooks:
1. **useAgentPackageList(params)** - Get all packages
   - Endpoint: `/api/haj-umrah/agent-packages`
   - Support: filtering, pagination

2. **useAgentPackage(id)** - Get single package
   - Endpoint: `/api/haj-umrah/agent-packages/:id`

3. **useCreateAgentPackage()** - Create package
   - Endpoint: `/api/haj-umrah/agent-packages` (POST)
   - Auto updates agent due amounts

4. **useUpdateAgentPackage()** - Update package
   - Endpoint: `/api/haj-umrah/agent-packages/:id` (PUT)

5. **useDeleteAgentPackage()** - Delete package
   - Endpoint: `/api/haj-umrah/agent-packages/:id` (DELETE)

6. **useAssignCustomersToPackage()** - Assign pilgrims
   - Endpoint: `/api/haj-umrah/agent-packages/:id/assign-customers` (POST)

7. **useRemoveCustomerFromPackage()** - Remove pilgrim
   - Endpoint: `/api/haj-umrah/agent-packages/:id/remove-customer/:customerId` (DELETE)

---

## Key Features:

1. **Auto Due Update**: Package create করলে agent-এর due amounts automatically update
2. **Agent Profile**: শুধু সেই agent-এর packages show করে
3. **Bangla Messages**: সব success/error messages বাংলায়
4. **Query Invalidation**: Package create/update করলে agent queries automatically refresh

---

## Usage Example:

```javascript
// In your component
import { useAgents, useAgent } from './hooks/useAgentQueries';
import { useCreateAgentPackage } from './hooks/useAgentPackageQueries';

function MyComponent() {
  // Get agents list
  const { data: agentsData } = useAgents(1, 100, '');
  
  // Get single agent with packages
  const { data: agentData } = useAgent('agent-id-here');
  
  // Create package mutation
  const createPackage = useCreateAgentPackage();
  
  // Create package
  const handleSubmit = (formData) => {
    createPackage.mutate(formData);
  };
  
  // Access agent packages
  const packages = agentData?.data?.packages || [];
  const dueAmounts = agentData?.data?.summary || {};
  
  return (
    <div>
      {packages.map(pkg => (
        <div key={pkg._id}>{pkg.packageName}</div>
      ))}
    </div>
  );
}
```
