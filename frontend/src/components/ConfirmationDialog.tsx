import React from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Alert,
  Divider,
} from '@mui/material';
import {
  Warning as WarningIcon,
  Delete as DeleteIcon,
  Archive as ArchiveIcon,
} from '@mui/icons-material';

interface ConfirmationDialogProps {
  open: boolean;
  onClose: () => void;
  onConfirm: () => void;
  operationType: 'ARCHIVE' | 'DELETE';
  operationData: {
    table?: string;
    count?: number;
    filters?: any;
    dateRange?: string;
  };
  loading?: boolean;
}

const ConfirmationDialog: React.FC<ConfirmationDialogProps> = ({
  open,
  onClose,
  onConfirm,
  operationType,
  operationData,
  loading = false,
}) => {
  const isDelete = operationType === 'DELETE';
  
  const getIcon = () => {
    return isDelete ? (
      <DeleteIcon sx={{ color: '#e53e3e', fontSize: 40 }} />
    ) : (
      <ArchiveIcon sx={{ color: '#3182ce', fontSize: 40 }} />
    );
  };

  const getTitle = () => {
    return isDelete ? 'Confirm Permanent Deletion' : 'Confirm Archive Operation';
  };

  const getDescription = () => {
    const { table, count, dateRange } = operationData;
    const action = isDelete ? 'permanently delete' : 'archive';
    
    return `You are about to ${action} **${count || 0}** records${table ? ` from ${table}` : ''}${dateRange ? ` (${dateRange})` : ''}.`;
  };

  const getWarning = () => {
    return isDelete 
      ? 'This action is PERMANENT and cannot be undone. The data will be completely removed from the system.'
      : 'Records will be moved to archive tables and removed from the main tables. This action can be reviewed in the audit log.';
  };

  const getConfirmText = () => {
    return isDelete ? 'DELETE PERMANENTLY' : 'CONFIRM ARCHIVE';
  };

  return (
    <Dialog
      open={open}
      onClose={onClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: {
          borderRadius: '12px',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
        },
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          {getIcon()}
          <Box>
            <Typography variant="h6" sx={{ fontWeight: 600, mb: 0.5 }}>
              {getTitle()}
            </Typography>
            <Typography variant="body2" color="text.secondary">
              Please review the operation details carefully
            </Typography>
          </Box>
        </Box>
      </DialogTitle>

      <Divider />

      <DialogContent sx={{ py: 3 }}>
        <Alert 
          severity={isDelete ? 'error' : 'warning'} 
          icon={<WarningIcon />}
          sx={{ mb: 3 }}
        >
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
            {getDescription()}
          </Typography>
          <Typography variant="body2">
            {getWarning()}
          </Typography>
        </Alert>

        {operationData.filters && (
          <Box sx={{ mt: 2 }}>
            <Typography variant="subtitle2" sx={{ fontWeight: 600, mb: 1 }}>
              Applied Filters:
            </Typography>
            <Box 
              component="pre" 
              sx={{ 
                fontSize: '0.85rem',
                background: '#f7fafc',
                padding: 2,
                borderRadius: 1,
                border: '1px solid #e2e8f0',
                overflow: 'auto',
                fontFamily: 'monospace',
              }}
            >
              {JSON.stringify(operationData.filters, null, 2)}
            </Box>
          </Box>
        )}

        <Box sx={{ mt: 3, p: 2, bgcolor: '#f7fafc', borderRadius: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 600, mb: 1 }}>
            What happens next:
          </Typography>
          <Box component="ul" sx={{ m: 0, pl: 2 }}>
            <Box component="li" sx={{ mb: 0.5 }}>
              <Typography variant="body2">
                Operation will be executed with full audit logging
              </Typography>
            </Box>
            <Box component="li" sx={{ mb: 0.5 }}>
              <Typography variant="body2">
                {isDelete ? 'Data will be permanently removed' : 'Data will be moved to archive tables'}
              </Typography>
            </Box>
            <Box component="li">
              <Typography variant="body2">
                You'll receive a confirmation message upon completion
              </Typography>
            </Box>
          </Box>
        </Box>
      </DialogContent>

      <Divider />

      <DialogActions sx={{ p: 3, gap: 2 }}>
        <Button
          onClick={onClose}
          variant="outlined"
          color="inherit"
          disabled={loading}
          sx={{ minWidth: 100 }}
        >
          Cancel
        </Button>
        <Button
          onClick={onConfirm}
          variant="contained"
          color={isDelete ? 'error' : 'primary'}
          disabled={loading}
          sx={{ 
            minWidth: 150,
            fontWeight: 600,
          }}
        >
          {loading ? 'Processing...' : getConfirmText()}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default ConfirmationDialog;