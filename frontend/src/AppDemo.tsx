import React from 'react';
import { Box, Typography, Button, Paper } from '@mui/material';
// ...existing code...

function App() {
  const [showDemo, setShowDemo] = React.useState(false);

  if (showDemo) {
    return (
      <Box sx={{ p: 2 }}>
        <Button 
          onClick={() => setShowDemo(false)}
          variant="contained"
          sx={{ mb: 2 }}
        >
          ← Back to Main App
        </Button>
        {/* StructuredContentDemo removed: component not found */}
      </Box>
    );
  }

  return (
    <Box sx={{ p: 2 }}>
      <Typography variant="h4" gutterBottom>
        Cloud Inventory DB Chatbot
      </Typography>
      <Paper sx={{ p: 2, mb: 2 }}>
        <Typography variant="body1" gutterBottom>
          Welcome! This is the main application. You can also view the structured content demo.
        </Typography>
        <Button 
          onClick={() => setShowDemo(true)}
          variant="outlined"
          sx={{ mr: 2 }}
        >
          View Structured Content Demo
        </Button>
        <Button 
          onClick={() => window.location.href = '/'}
          variant="contained"
        >
          Continue to Chat
        </Button>
      </Paper>
    </Box>
  );
}

export default App;