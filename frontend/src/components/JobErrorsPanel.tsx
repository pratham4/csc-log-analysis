import React, { useState, useEffect } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  Alert,
  CircularProgress,
  Button,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
  Tooltip
} from '@mui/material';
import { 
  Error as ErrorIcon, 
  Refresh as RefreshIcon,
  AccessTime as TimeIcon,
  Info as InfoIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon
} from '@mui/icons-material';
import { apiService } from '../services/api';
import type { JobLogRecord } from '../services/api';

interface JobErrorsPanelProps {
  selectedRegion: string | null;
}

const JobErrorsPanel: React.FC<JobErrorsPanelProps> = ({ selectedRegion }) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [errorJobs, setErrorJobs] = useState<JobLogRecord[]>([]);
  const [totalErrors, setTotalErrors] = useState(0);
  const [selectedJob, setSelectedJob] = useState<JobLogRecord | null>(null);
  const [jobDetailLoading, setJobDetailLoading] = useState(false);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [errorDetailsExpanded, setErrorDetailsExpanded] = useState(false);

  useEffect(() => {
    if (selectedRegion) {
      loadLatestErrors();
    } else {
      setErrorJobs([]);
      setTotalErrors(0);
    }
  }, [selectedRegion]);

  const loadLatestErrors = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await apiService.getLatestErrorJobs(3);
      
      if (response.success) {
        setErrorJobs(response.records);
        setTotalErrors(response.total_errors);
      } else {
        setError('Failed to load error jobs');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load error jobs');
      console.error('Error loading latest error jobs:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleJobClick = async (job: JobLogRecord) => {
    try {
      setJobDetailLoading(true);
      setSelectedJob(job);
      setDialogOpen(true);
      setErrorDetailsExpanded(false); // Reset expansion state
      
      // Load more detailed information if needed
      const detailResponse = await apiService.getJobDetail(job.id);
      if (detailResponse.success) {
        setSelectedJob(detailResponse.record);
      }
    } catch (err) {
      console.error('Error loading job detail:', err);
    } finally {
      setJobDetailLoading(false);
    }
  };

  const formatDateTime = (dateTimeStr: string | null) => {
    if (!dateTimeStr) return 'N/A';
    try {
      return new Date(dateTimeStr).toLocaleString();
    } catch {
      return dateTimeStr;
    }
  };

  const getJobTypeColor = (jobType: string) => {
    switch (jobType) {
      case 'DELETE': return 'error';
      case 'ARCHIVE': return 'warning';
      default: return 'default';
    }
  };

  const getSourceColor = (source: string) => {
    return source === 'CHATBOT' ? 'primary' : 'secondary';
  };

  const handleDialogClose = () => {
    setDialogOpen(false);
    setErrorDetailsExpanded(false);
    setSelectedJob(null);
  };

  if (!selectedRegion) {
    return (
      <Card 
        sx={{ 
          borderRadius: '8px',
          background: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 100%)',
          boxShadow: '0 20px 40px rgba(0, 0, 0, 0.1), 0 0 0 1px rgba(255, 255, 255, 0.2)',
          border: 'none',
          borderTop: '4px solid #00A9CE',
          overflow: 'hidden',
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center', // Center horizontally
        }}
      >
        <CardContent sx={{ 
          p: 3, 
          textAlign: 'center',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          width: '100%'
        }}>
          <ErrorIcon sx={{ fontSize: 48, color: '#00A9CE', mb: 2 }} />
          <Typography variant="h6" sx={{ color: '#64748b', fontWeight: 600 }}>
            Job Errors
          </Typography>
          <Typography variant="body2" sx={{ color: '#94a3b8', mt: 1 }}>
            Select a region to view error logs
          </Typography>
        </CardContent>
      </Card>
    );
  }

  return (
    <>
      <Card 
        sx={{ 
          borderRadius: '8px',
          background: 'linear-gradient(145deg, #ffffff 0%, #f8fafc 100%)',
          boxShadow: '0 20px 40px rgba(0, 0, 0, 0.1), 0 0 0 1px rgba(255, 255, 255, 0.2)',
          border: 'none',
          borderTop: '4px solid #00A9CE',
          overflow: 'hidden',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
        }}
      >
        <CardContent sx={{ p: 0, height: '100%', display: 'flex', flexDirection: 'column' }}>
          {/* Header */}
          <Box sx={{ 
            p: 3,
            pb: 2,
            background: 'linear-gradient(135deg, rgba(37, 99, 235, 0.05) 0%, rgba(124, 58, 237, 0.05) 100%)',
            borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
            flex: '0 0 auto', // Fixed header
          }}>
            <Box display="flex" alignItems="center" justifyContent="space-between">
              <Box display="flex" alignItems="center">
                <ErrorIcon sx={{ color: '#00A9CE', mr: 1, fontSize: 24 }} />
                <Box>
                  <Typography variant="h6" sx={{ 
                    fontWeight: 700, 
                    color: '#0f172a',
                    fontSize: '1.1rem',
                    mb: 0.5
                  }}>
                    Job Errors
                  </Typography>
                  <Typography variant="body2" sx={{ 
                    color: '#333333', 
                    fontSize: '0.875rem' 
                  }}>
                    {totalErrors} errors in 24 hours
                  </Typography>
                </Box>
              </Box>
              <Button
                variant="outlined"
                size="small"
                onClick={loadLatestErrors}
                disabled={loading}
                sx={{ 
                  borderRadius: '8px',
                  borderColor: '#00A9CE',
                  color: '#00A9CE',
                  fontSize: '0.75rem',
                  fontWeight: 500,
                  textTransform: 'none',
                  minWidth: 'auto',
                  width: '40px',
                  height: '32px',
                  px: 1,
                  '&:hover': {
                    backgroundColor: 'rgba(0, 169, 206, 0.08) !important',
                    borderColor: '#00A9CE !important',
                  },
                }}
              >
                {loading ? <CircularProgress size={16} sx={{ color: '#00A9CE' }} /> : <RefreshIcon sx={{ fontSize: 16 }} />}
              </Button>
            </Box>
          </Box>

          {/* Content */}
          <Box sx={{ 
            p: 3, 
            pt: 2, 
            flex: '1 1 auto', 
            overflow: 'auto',
            minHeight: 0, // Important for flex scrolling
            // Custom scrollbar styling
            '&::-webkit-scrollbar': {
              width: '6px',
            },
            '&::-webkit-scrollbar-track': {
              backgroundColor: 'transparent',
            },
            '&::-webkit-scrollbar-thumb': {
              backgroundColor: 'rgba(0, 0, 0, 0.2)',
              borderRadius: '3px',
              '&:hover': {
                backgroundColor: 'rgba(0, 0, 0, 0.3)',
              },
            },
          }}>
            {error && (
              <Alert 
                severity="warning" 
                sx={{ 
                  mb: 2,
                  borderRadius: '12px',
                  backgroundColor: '#fffbeb',
                  border: '1px solid #f59e0b',
                  '& .MuiAlert-icon': {
                    color: '#f59e0b',
                  },
                }}
              >
                <Typography sx={{ fontSize: '0.85rem', fontWeight: 500 }}>
                  {error}
                </Typography>
              </Alert>
            )}

            {loading ? (
              <Box 
                display="flex" 
                flexDirection="column"
                justifyContent="center" 
                alignItems="center" 
                py={4}
                sx={{ width: '100%' }}
              >
                <CircularProgress size={24} sx={{ color: '#00A9CE', mb: 1 }} />
                <Typography variant="body2" sx={{ color: '#64748b', textAlign: 'center' }}>
                  Loading error jobs...
                </Typography>
              </Box>
            ) : errorJobs.length === 0 ? (
              <Box 
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                textAlign="center" 
                py={4}
                sx={{ width: '100%' }}
              >
                <Typography variant="body2" sx={{ color: '#64748b' }}>
                  No errors in the last 24 hours
                </Typography>
              </Box>
            ) : (
              <Box>
                {errorJobs.map((job, index) => (
                  <Box key={job.id} mb={index < errorJobs.length - 1 ? 2 : 0}>
                    <Card
                      sx={{
                        cursor: 'pointer',
                        border: '1px solid rgba(0, 169, 206, 0.3)',
                        borderRadius: '8px',
                        transition: 'all 0.2s ease-in-out',
                        backgroundColor: 'white',
                        boxShadow: '0 2px 8px rgba(0, 0, 0, 0.08)',
                        '&:hover': {
                          borderColor: 'rgba(0, 169, 206, 0.5)',
                          boxShadow: '0 4px 12px rgba(0, 169, 206, 0.15)',
                          transform: 'translateY(-1px)',
                        },
                      }}
                      onClick={() => handleJobClick(job)}
                    >
                      <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                        <Box display="flex" alignItems="flex-start" justifyContent="space-between" mb={1}>
                          <Typography variant="subtitle2" sx={{ fontWeight: 600, color: '#00A9CE', fontSize: '0.9rem' }}>
                            {job.table_name}
                          </Typography>
                          <Box display="flex" gap={0.5}>
                            <Chip 
                              label={job.job_type} 
                              size="small"
                              color={getJobTypeColor(job.job_type) as any}
                              sx={{ fontSize: '0.65rem', height: '20px' }}
                            />
                            <Chip 
                              label={job.source} 
                              size="small"
                              color={getSourceColor(job.source) as any}
                              sx={{ fontSize: '0.65rem', height: '20px' }}
                            />
                          </Box>
                        </Box>
                        
                        <Typography 
                          variant="body2" 
                          sx={{ 
                            color: '#64748b', 
                            fontSize: '0.8rem',
                            mb: 1,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            display: '-webkit-box',
                            WebkitLineClamp: 2,
                            WebkitBoxOrient: 'vertical',
                          }}
                        >
                          {job.reason || 'No error details available'}
                        </Typography>
                        
                        <Box display="flex" alignItems="center" justifyContent="space-between" mt={1}>
                          <Box display="flex" alignItems="center" gap={1}>
                            <TimeIcon sx={{ fontSize: 12, color: '#94a3b8' }} />
                            <Typography variant="caption" sx={{ color: '#94a3b8', fontSize: '0.7rem' }}>
                              {formatDateTime(job.started_at)}
                            </Typography>
                          </Box>
                          <Tooltip title="Click for details">
                            <InfoIcon sx={{ fontSize: 14, color: '#94a3b8' }} />
                          </Tooltip>
                        </Box>
                      </CardContent>
                    </Card>
                  </Box>
                ))}
              </Box>
            )}
          </Box>
        </CardContent>
      </Card>

      {/* Job Detail Dialog */}
      <Dialog 
        open={dialogOpen} 
        onClose={handleDialogClose}
        maxWidth="sm"
        fullWidth
        PaperProps={{
          sx: {
            borderRadius: '12px',
            maxHeight: errorDetailsExpanded ? '85vh' : '70vh',
            transition: 'max-height 0.3s ease-in-out'
          }
        }}
      >
        <DialogTitle sx={{ 
          pb: 1,
          borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
          background: 'linear-gradient(135deg, rgba(37, 99, 235, 0.05) 0%, rgba(124, 58, 237, 0.05) 100%)',
        }}>
          <Box display="flex" alignItems="center">
            <ErrorIcon sx={{ color: '#00A9CE', mr: 1 }} />
            <Typography variant="h6" sx={{ fontWeight: 600 }}>
              Error Details
            </Typography>
          </Box>
        </DialogTitle>
        
        <DialogContent sx={{ 
          p: 3, 
          overflow: 'auto',
          // Custom scrollbar for dialog
          '&::-webkit-scrollbar': {
            width: '6px',
          },
          '&::-webkit-scrollbar-track': {
            backgroundColor: 'transparent',
          },
          '&::-webkit-scrollbar-thumb': {
            backgroundColor: 'rgba(0, 0, 0, 0.2)',
            borderRadius: '3px',
            '&:hover': {
              backgroundColor: 'rgba(0, 0, 0, 0.3)',
            },
          },
        }}>
          {jobDetailLoading ? (
            <Box 
              display="flex" 
              flexDirection="column"
              justifyContent="center" 
              alignItems="center" 
              py={4}
              sx={{ width: '100%' }}
            >
              <CircularProgress sx={{ mb: 1 }} />
              <Typography variant="body2" sx={{ color: '#64748b', textAlign: 'center' }}>
                Loading job details...
              </Typography>
            </Box>
          ) : selectedJob && (
            <Box>
              {/* Main Details - Simplified */}
              <Box display="grid" gridTemplateColumns="1fr 1fr" gap={3} mb={3}>
                <Box>
                  <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 500, textTransform: 'uppercase' }}>
                    Job ID
                  </Typography>
                  <Typography variant="h6" sx={{ fontWeight: 600, color: '#0f172a' }}>
                    #{selectedJob.id}
                  </Typography>
                </Box>
                <Box>
                  <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 500, textTransform: 'uppercase' }}>
                    Table
                  </Typography>
                  <Typography variant="h6" sx={{ fontWeight: 600, color: '#0f172a' }}>
                    {selectedJob.table_name}
                  </Typography>
                </Box>
                <Box>
                  <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 500, textTransform: 'uppercase' }}>
                    When
                  </Typography>
                  <Typography variant="body1" sx={{ fontWeight: 600, color: '#0f172a' }}>
                    {formatDateTime(selectedJob.started_at)}
                  </Typography>
                </Box>
                <Box>
                  <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 500, textTransform: 'uppercase' }}>
                    Records
                  </Typography>
                  <Typography variant="body1" sx={{ fontWeight: 600, color: '#0f172a' }}>
                    {selectedJob.records_affected.toLocaleString()}
                  </Typography>
                </Box>
              </Box>
              
              <Box display="flex" gap={1} mb={3}>
                <Chip 
                  label={selectedJob.job_type} 
                  color={getJobTypeColor(selectedJob.job_type) as any}
                  sx={{ fontWeight: 600 }}
                />
                <Chip 
                  label={selectedJob.source} 
                  color={getSourceColor(selectedJob.source) as any}
                  sx={{ fontWeight: 600 }}
                />
                <Chip 
                  label={selectedJob.status} 
                  color="error"
                  sx={{ fontWeight: 600 }}
                />
              </Box>

              {/* Error Summary - Expandable */}
              <Box>
                <Box display="flex" alignItems="center" justifyContent="space-between">
                  <Typography variant="caption" sx={{ color: '#64748b', fontWeight: 500, textTransform: 'uppercase' }}>
                    Error Details
                  </Typography>
                  {selectedJob.reason && (selectedJob.reason.length > 200 || selectedJob.reason.includes('\n')) && (
                    <Button
                      size="small"
                      onClick={() => setErrorDetailsExpanded(!errorDetailsExpanded)}
                      startIcon={errorDetailsExpanded ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                      sx={{
                        fontSize: '0.75rem',
                        fontWeight: 500,
                        textTransform: 'none',
                        color: '#ef4444',
                        minWidth: 'auto',
                        p: 0.5,
                        '&:hover': {
                          backgroundColor: 'rgba(239, 68, 68, 0.08)',
                        },
                      }}
                    >
                      {errorDetailsExpanded ? 'Show Less' : 'Show More'}
                    </Button>
                  )}
                </Box>
                <Box 
                  sx={{ 
                    p: 2, 
                    backgroundColor: '#fef2f2', 
                    border: '1px solid #ef4444',
                    borderRadius: '8px',
                    mt: 1
                  }}
                >
                  <Typography 
                    variant="body2" 
                    sx={{ 
                      color: '#7f1d1d',
                      fontWeight: 500,
                      whiteSpace: errorDetailsExpanded ? 'pre-wrap' : 'normal',
                      wordBreak: 'break-word',
                      transition: 'all 0.3s ease-in-out',
                      ...(errorDetailsExpanded ? {} : {
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        display: '-webkit-box',
                        WebkitLineClamp: 3,
                        WebkitBoxOrient: 'vertical',
                      })
                    }}
                  >
                    {selectedJob.reason || 'No error details available'}
                  </Typography>
                </Box>
              </Box>
            </Box>
          )}
        </DialogContent>
        
        <DialogActions sx={{ p: 3, pt: 1, borderTop: '1px solid rgba(0, 0, 0, 0.08)' }}>
          <Button 
            onClick={handleDialogClose}
            sx={{ 
              borderRadius: '8px',
              fontWeight: 600,
              textTransform: 'none'
            }}
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </>
  );
};

export default JobErrorsPanel;