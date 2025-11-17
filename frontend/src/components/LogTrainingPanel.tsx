import React from 'react';
import { Box, Typography, Button, LinearProgress } from '@mui/material';

interface LogTrainingPanelProps {
  trainingStatus?: number;
  onUpload?: (files: FileList) => void;
}

const LogTrainingPanel: React.FC<LogTrainingPanelProps> = ({ trainingStatus = 0, onUpload }) => {
  return (
    <Box mb={2}>
      <Typography variant="h6">Healthy Logs Training</Typography>
      <Button variant="contained" component="label">
        Upload Healthy Logs
        <input type="file" multiple hidden onChange={e => onUpload && e.target.files && onUpload(e.target.files)} />
      </Button>
      <Box mt={1}>
        <Typography variant="body2">Patterns trained: {trainingStatus}</Typography>
        <LinearProgress variant="determinate" value={Math.min(trainingStatus, 100)} />
      </Box>
    </Box>
  );
};

export default LogTrainingPanel;
