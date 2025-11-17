import React from 'react';
import { Box, Typography, Chip } from '@mui/material';

interface S3StatusIndicatorProps {
  logCount?: number;
}

const S3StatusIndicator: React.FC<S3StatusIndicatorProps> = ({ logCount = 0 }) => (
  <Box mb={2}>
    <Typography variant="h6">S3 Bucket Status</Typography>
    <Chip label={`Logs in S3: ${logCount}`} color={logCount > 0 ? 'primary' : 'default'} />
  </Box>
);

export default S3StatusIndicator;
