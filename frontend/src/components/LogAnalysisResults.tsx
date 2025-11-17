import React from 'react';
import { Box, Typography, Paper, List, ListItem, ListItemText } from '@mui/material';
import type { LogAnalysisResult } from '../types/logAnalysis';

interface LogAnalysisResultsProps {
  results: LogAnalysisResult[];
}

const LogAnalysisResults: React.FC<LogAnalysisResultsProps> = ({ results }) => (
  <Box mb={2}>
    <Typography variant="h6">Analysis Results</Typography>
    <Paper elevation={2}>
      <List>
        {results.map((result, idx) => (
          <ListItem key={idx}>
            <ListItemText
              primary={result.log}
              secondary={`Unhealthy: ${result.unhealthy ? 'Yes' : 'No'} | Score: ${result.score} | Keywords: ${result.matchedKeywords.join(', ')}${result.suggestedSolution ? ' | Solution: ' + result.suggestedSolution : ''}`}
            />
          </ListItem>
        ))}
      </List>
    </Paper>
  </Box>
);

export default LogAnalysisResults;
