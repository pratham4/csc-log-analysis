import React from 'react';
import { Box, Typography, Checkbox, FormControlLabel } from '@mui/material';
import type { LogAnalysisResult } from '../types/logAnalysis';

interface LogFilterPanelProps {
  results: LogAnalysisResult[];
  showUnhealthyOnly: boolean;
  onToggle: (checked: boolean) => void;
}

const LogFilterPanel: React.FC<LogFilterPanelProps> = ({ results, showUnhealthyOnly, onToggle }) => (
  <Box mb={2}>
    <Typography variant="h6">Log Filter</Typography>
    <FormControlLabel
      control={<Checkbox checked={showUnhealthyOnly} onChange={e => onToggle(e.target.checked)} />}
      label="Show Unhealthy Logs Only"
    />
    <Typography variant="body2">Total logs: {results.length}</Typography>
  </Box>
);

export default LogFilterPanel;
