import React, { useState, useEffect, useRef } from 'react';
import {
  Box,
  Card,
  CardContent,
  TextField,
  Button,
  Typography,
  Chip,
  Paper,
  IconButton,
  CircularProgress,
} from '@mui/material';
import {
  Send as SendIcon,
  Refresh as RefreshIcon,
} from '@mui/icons-material';
import { apiService, type ChatResponse, type ChatMessage } from '../services/api';
import type { Region } from '../types/region';
import StructuredContentRenderer from './StructuredContentRenderer';

interface Message {
  id: string;
  text: string;
  isBot: boolean;
  timestamp: Date;
  suggestions?: string[];
  requiresConfirmation?: boolean;
  operationData?: any;
  structuredContent?: any;  // For structured content rendering
}
interface ChatBotProps {
  userId: string;
  userRole: string;
  selectedRegion: Region | null;
  regionStatus: Record<string, boolean>;
}

export const ChatBot: React.FC<ChatBotProps> = ({ userId, userRole, selectedRegion, regionStatus }) => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputText, setInputText] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId] = useState(() => `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`);
  const messageCounterRef = useRef(0); // Add counter for unique message IDs
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const isRegionConnected = () => {
    return selectedRegion ? regionStatus[selectedRegion] === true : false;
  };

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const [lastInitializedRegion, setLastInitializedRegion] = useState<Region | null | undefined>(undefined);
  const [isInitializing, setIsInitializing] = useState(false);

  useEffect(() => {
    const shouldSendInitialMessage = 
      !isInitializing &&
      (lastInitializedRegion === undefined || selectedRegion !== lastInitializedRegion);

    if (shouldSendInitialMessage) {
      if (lastInitializedRegion !== undefined) {
        setMessages([]);
      }
      
      sendInitialMessage();
      setLastInitializedRegion(selectedRegion);
    }
  }, [selectedRegion, isInitializing]);

  const sendInitialMessage = async () => {
    if (isInitializing) {
      return;
    }

    if (messages.length > 0 && lastInitializedRegion === selectedRegion) {
      return;
    }

    try {
      setIsInitializing(true);
      
      const initialMessage = isRegionConnected() 
        ? `Hello! I'm logged in as ${userRole} role. I'm working with region ${selectedRegion}.`
        : selectedRegion 
          ? `Hello! I'm logged in as ${userRole} role. Region ${selectedRegion} is selected but not connected.`
          : `Hello! I'm logged in as ${userRole} role.`;
        
      const response = await apiService.chatWithAgent({
        message: initialMessage,
        user_id: userId,
        session_id: sessionId,
        region: selectedRegion || undefined,
      });
      
      // Use the actual response from the backend with structured content
      addBotMessage(response);
      
    } catch (error) {
      console.error('Error sending initial message:', error);
      const roleCapabilities = userRole === 'Admin' 
        ? 'you have access to all operations including archiving and deletion'
        : 'you have read-only access for viewing data';
        
      const connectionMessage = isRegionConnected() 
        ? `Currently connected to ${selectedRegion} region`
        : selectedRegion 
          ? `Region ${selectedRegion} is selected. Please connect to the region first.`
          : 'Please select and connect to a region to get started';

      addBotMessage({
        response: `Hello ${userId}! I'm your Cloud Inventory Assistant. As a ${userRole}, ${roleCapabilities}. ${connectionMessage}.`,
        requires_confirmation: false,
      });
    } finally {
      setIsInitializing(false);
    }
  };

  const addBotMessage = (response: ChatResponse) => {
    messageCounterRef.current += 1;
    const botMessage: Message = {
      id: `bot_${sessionId}_${messageCounterRef.current}`,
      text: response.response,
      isBot: true,
      timestamp: new Date(),
      suggestions: response.suggestions,
      requiresConfirmation: response.requires_confirmation,
      operationData: response.operation_data,
      structuredContent: response.structured_content,
    };
    
    setMessages(prev => [...prev, botMessage]);
  };

  const addUserMessage = (text: string) => {
    messageCounterRef.current += 1;
    const userMessage: Message = {
      id: `user_${sessionId}_${messageCounterRef.current}`,
      text,
      isBot: false,
      timestamp: new Date(),
    };
    
    setMessages(prev => [...prev, userMessage]);
  };

  const sendMessage = async (messageText: string = inputText) => {
    if (!messageText.trim() || isLoading) return;

    // Check if region is connected for database operations
    if (!isRegionConnected() && 
        (messageText.toLowerCase().includes('show') || 
         messageText.toLowerCase().includes('count') || 
         messageText.toLowerCase().includes('archive') || 
         messageText.toLowerCase().includes('delete') ||
         messageText.toLowerCase().includes('statistics'))) {
      
      addBotMessage({
        response: selectedRegion 
          ? `Region ${selectedRegion} is selected. Please connect to the region first to perform database operations.`
          : 'No region is connected. Please select and connect to a region first to perform database operations.',
        requires_confirmation: false,
      });
      return;
    }

    setIsLoading(true);
    addUserMessage(messageText);
    setInputText('');

    try {
      const chatMessage: ChatMessage = {
        message: messageText,
        user_id: userId,
        session_id: sessionId,
        region: isRegionConnected() ? selectedRegion : undefined,
      };

      const response = await apiService.chatWithAgent(chatMessage);
      
      addBotMessage(response);
    } catch (error) {
      console.error('Error sending message:', error);
      addBotMessage({
        response: `Sorry, I encountered an error: ${error instanceof Error ? error.message : 'Unknown error'}. Please try again.`,
        requires_confirmation: false,
      });
    } finally {
      setIsLoading(false);
    }
  };

  const handleSuggestionClick = (suggestion: string) => {
    // Directly send the suggestion as a message instead of just setting input text
    sendMessage(suggestion);
  };

  const handleDirectConfirmation = async (
    operation: string, 
    confirmed: boolean,
    operationData: any
  ) => {
    if (!selectedRegion) {
      addBotMessage({
        response: 'No region selected. Please select a region first.',
        requires_confirmation: false,
      });
      return;
    }

    setIsLoading(true);
    
    // Add user message showing what they clicked
    const actionText = confirmed 
      ? `CONFIRM ${operation}` 
      : 'CANCEL';
    addUserMessage(actionText);

    try {
      const confirmationRequest = {
        operation: operation,
        table: operationData.table || '',
        region: selectedRegion,
        filters: operationData.filters || {},
        confirmed: confirmed
      };

      const response = await apiService.confirmOperation(confirmationRequest);
      addBotMessage(response);
    } catch (error) {
      console.error('Error sending confirmation:', error);
      addBotMessage({
        response: `Sorry, I encountered an error: ${error instanceof Error ? error.message : 'Unknown error'}. Please try again.`,
        requires_confirmation: false,
      });
    } finally {
      setIsLoading(false);
    }
  };



  const handleKeyPress = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      sendMessage();
    }
  };

  const formatTimestamp = (timestamp: Date) => {
    return timestamp.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      hour12: true 
    });
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Main Chat Area */}
      <Card 
        sx={{ 
          flex: 1,
          display: 'flex', 
          flexDirection: 'column',
          borderRadius: '0px',
          background: '#ffffff',
          boxShadow: 'none',
          border: 'none',
          borderTop: '4px solid #00A9CE',
          overflow: 'hidden',
          height: '100%',
        }}
      >
      <CardContent sx={{ 
        flexGrow: 1, 
        overflow: 'hidden', 
        display: 'flex', 
        flexDirection: 'column',
        p: 0,
      }}>
        {/* Modern Header */}
        <Box sx={{ 
          display: 'flex', 
          alignItems: 'center', 
          p: 3,
          pb: 2,
          background: 'linear-gradient(135deg, rgba(0, 188, 212, 0.05) 0%, rgba(31, 76, 95, 0.05) 100%)',
          borderBottom: '1px solid rgba(0, 0, 0, 0.08)',
        }}>
          <Box
            sx={{
              width: 48,
              height: 48,
              borderRadius: '50%',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              mr: 2,
            }}
          >
            <img 
              src="/cloud_bot_white.svg" 
              alt="Cloud Inventory Assistant" 
              style={{ width: 48, height: 48 }}
            />
          </Box>
          <Box sx={{ flexGrow: 1 }}>
            <Typography variant="h6" component="h2" sx={{ fontWeight: 700, mb: 0.5 }}>
              Cloud Inventory Assistant
            </Typography>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              {selectedRegion ? (
                <>
                  <Typography 
                    sx={{ 
                      fontSize: '0.875rem',
                      fontWeight: 'bold',
                      color: 'black'
                    }}
                  >
                    {selectedRegion.toUpperCase()}
                  </Typography>
                  <Box
                    sx={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 0.5,
                      backgroundColor: isRegionConnected() ? 'rgba(34, 197, 94, 0.1)' : 'rgba(236, 112, 112, 0.2)',
                      color: isRegionConnected() ? '#22c55e' : '#ef4444',
                      px: 1,
                      py: 0.25,
                      borderRadius: '12px',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                    }}
                  >
                    <Box
                      sx={{
                        width: 8,
                        height: 8,
                        borderRadius: '50%',
                        backgroundColor: isRegionConnected() ? '#22c55e' : '#ef4444',
                      }}
                    />
                    <span>{isRegionConnected() ? 'Connected' : 'Disconnected'}</span>
                  </Box>
                </>
              ) : (
                <Box
                  sx={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 0.5,
                    backgroundColor: isRegionConnected() ? 'rgba(34, 197, 94, 0.1)' : 'rgba(236, 112, 112, 0.2)',
                    color: isRegionConnected() ? '#22c55e' : '#ef4444',
                    px: 1.5,
                    py: 0.5,
                    borderRadius: '12px',
                    fontSize: '0.75rem',
                    fontWeight: 600,
                  }}
                >
                  <Box
                    sx={{
                      width: 6,
                      height: 6,
                      borderRadius: '50%',
                      color: isRegionConnected() ? '#22c55e' : '#ef4444',
                    }}
                  />
                  <span>No region selected</span>
                </Box>
              )}
            </Box>
          </Box>
          <IconButton 
            size="small" 
            onClick={() => {
              setMessages([]);
              setLastInitializedRegion(undefined);
              setIsInitializing(false);
            }}
            title="Restart chat"
            sx={{
              backgroundColor: 'rgba(42, 79, 160, 0.1)',
              borderRadius: '5px',
              color: 'primary.main',
              '&:hover': {
                backgroundColor: 'rgba(42, 79, 160, 0.2)',
              },
            }}
          >
            <RefreshIcon fontSize="small" />
          </IconButton>
        </Box>

        {/* Messages Container */}
        <Box sx={{ 
          flexGrow: 1, 
          overflow: 'auto', 
          p: 3,
          pt: 2,
          background: '#ffffff',
          '&::-webkit-scrollbar': {
            width: '6px',
          },
          '&::-webkit-scrollbar-track': {
            background: 'transparent',
          },
          '&::-webkit-scrollbar-thumb': {
            background: 'rgba(0, 0, 0, 0.2)',
            borderRadius: '3px',
            '&:hover': {
              background: 'rgba(0, 0, 0, 0.3)',
            },
          },
        }}>
          {messages.map((message) => (
            <Box
              key={message.id}
              sx={{
                display: 'flex',
                justifyContent: message.isBot ? 'flex-start' : 'flex-end',
                mb: 2,
                width: '100%',
              }}
            >
              <Paper
                elevation={0}
                sx={{
                  maxWidth: message.isBot ? '95%' : '85%',
                  minWidth: message.isBot && message.structuredContent ? '300px' : 'auto',
                  width: message.isBot && message.structuredContent ? '100%' : 'auto',
                  p: 2,
                  background: message.isBot 
                    ? 'transparent'
                    : '#00A9CE',
                  color: message.isBot ? '#000000' : 'white',
                  borderRadius: message.isBot ? '20px 20px 20px 6px' : '20px 20px 6px 20px',
                  border: message.isBot 
                    ? 'none'
                    : 'none',
                  boxShadow: message.isBot 
                    ? 'none'
                    : '0 4px 20px rgba(0, 169, 206, 0.3)',
                  overflow: 'hidden',
                }}
              >
                <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5, width: '100%' }}>
                  <Box
                    sx={{
                      width: 32,
                      height: 32,
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      flexShrink: 0,
                      mt: 0.2,
                    }}
                  >
                    {message.isBot ? (
                      <></>
                    ) : (
                      <Box
                        sx={{
                          width: 40,
                          height: 40,
                          borderRadius: '50%',
                          backgroundColor: '#00A9CE',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                        }}
                      >
                        <img 
                          src="/user_logo_dark.svg" 
                          alt="User" 
                          style={{ width: 40, height: 40 }}
                        />
                      </Box>
                    )}
                  </Box>
                  <Box sx={{ flexGrow: 1, minWidth: 0, width: '100%' }}> {/* Ensure content can use full width */}
                    {/* Render structured content if available, otherwise plain text */}
                    {message.isBot && message.structuredContent ? (
                      <>
                        <Box sx={{ mb: 1 }}>
                          <StructuredContentRenderer 
                            content={message.structuredContent} 
                            onSuggestionClick={handleSuggestionClick}
                            onDirectConfirmation={handleDirectConfirmation}
                          />
                        </Box>
                      </>
                    ) : (
                      <Typography variant="body2" sx={{ 
                        whiteSpace: 'pre-wrap', 
                        lineHeight: 1.5,
                        wordBreak: 'break-word',
                        fontWeight: message.isBot ? 600 : 'normal',
                        fontSize: message.isBot ? '1rem' : '0.875rem',
                      }}>
                        {message.text}
                      </Typography>
                    )}
                    <Typography 
                      variant="caption" 
                      sx={{ 
                        opacity: 0.7, 
                        display: 'block', 
                        mt: 1,
                        fontSize: '0.75rem',
                      }}
                    >
                      {formatTimestamp(message.timestamp)}
                    </Typography>
                  </Box>
                </Box>
              </Paper>
            </Box>
          ))}

          {isLoading && (
            <Box sx={{ display: 'flex', justifyContent: 'flex-start', mb: 2 }}>
              <Paper 
                elevation={0} 
                sx={{ 
                  p: 2, 
                  backgroundColor: '#F0F0F0',
                  borderRadius: '20px 20px 20px 6px',
                  border: 'none',
                  boxShadow: 'none',
                }}
              >
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
                  <Box
                    sx={{
                      width: 40,
                      height: 40,
                      borderRadius: '50%',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                    }}
                  >
                    <img 
                      src="/cloud_bot_colored.svg" 
                      alt="AI" 
                      style={{ width: 40, height: 40 }}
                    />
                  </Box>
                  <CircularProgress size={16} sx={{ color: 'primary.main' }} />
                  <Typography variant="body2" sx={{ fontStyle: 'italic', color: 'text.secondary' }}>
                    Thinking...
                  </Typography>
                </Box>
              </Paper>
            </Box>
          )}

          <div ref={messagesEndRef} />
        </Box>

        {/* Modern Input Area */}
        <Box sx={{ 
          p: 3, 
          pt: 2,
          background: '#ffffff',
          borderTop: '1px solid rgba(0, 0, 0, 0.08)',
        }}>
          <Box sx={{ 
            display: 'flex', 
            alignItems: 'center',
            gap: 2,
          }}>
            <Box sx={{ 
              flex: 1,
              backgroundColor: '#f8fafc',
              borderRadius: '25px',
              border: '1px solid #00bcd4',
              boxShadow: '0 2px 12px rgba(0, 0, 0, 0.08)',
              transition: 'all 0.2s ease-in-out',
              '&:focus-within': {
                border: '2px solid #00bcd4',
                boxShadow: '0 4px 20px rgba(0, 188, 212, 0.3)',
                backgroundColor: '#ffffff',
              }
            }}>
              <TextField
                fullWidth
                size="medium"
                placeholder="Type here..."
                value={inputText}
                onChange={(e) => setInputText(e.target.value)}
                onKeyPress={handleKeyPress}
                disabled={isLoading}
                multiline
                maxRows={3}
                inputRef={inputRef}
                sx={{
                  '& .MuiOutlinedInput-root': {
                    border: 'none',
                    borderRadius: '25px',
                    backgroundColor: 'transparent',
                    boxShadow: 'none',
                    pl: 3,
                    pr: 3,
                    '& fieldset': {
                      border: 'none',
                    },
                    '&:hover fieldset': {
                      border: 'none',
                    },
                    '&.Mui-focused fieldset': {
                      border: 'none',
                    },
                  },
                  '& .MuiInputBase-input': {
                    fontSize: '0.95rem',
                    lineHeight: 1.5,
                    color: '#374151',
                    '&::placeholder': {
                      color: '#9ca3af',
                      opacity: 1,
                    },
                  },
                }}
              />
            </Box>
            <Button
              variant="contained"
              size="large"
              onClick={() => sendMessage()}
              disabled={!inputText.trim() || isLoading}
              sx={{
                minWidth: '48px',
                width: '48px',
                height: '48px',
                borderRadius: '50%',
                background: '#3db7f0ff',
                color: '#ffffff',
                boxShadow: '0 2px 8px rgba(179, 229, 252, 0.4)',
                border: 'none',
                '&:hover': {
                  boxShadow: '0 4px 12px rgba(129, 212, 250, 0.5)',
                  transform: 'translateY(-1px)',
                },
                '&:disabled': {
                  background: '#B3E5FC',
                  color: '#ffffff',
                  boxShadow: 'none',
                  transform: 'none',
                },
                transition: 'all 0.2s ease-in-out',
              }}
            >
              <SendIcon fontSize="small" sx={{ color: '#ffffff' }} />
            </Button>
          </Box>

          {/* General Prompts - Quick Access */}
          <Box sx={{ mt: 2 }}>
            <Typography 
              variant="caption" 
              sx={{ 
                opacity: 0.8, 
                mb: 1.5, 
                display: 'block',
                fontWeight: 600,
                textTransform: 'uppercase',
                fontSize: '0.7rem',
                letterSpacing: '0.05em',
              }}
            >
              Quick Actions
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
              <Chip
                label="Show table statistics"
                size="small"
                variant="outlined"
                clickable
                onClick={() => handleSuggestionClick("Show table statistics")}
                sx={{ 
                  fontSize: '0.75rem',
                  height: '28px',
                  borderRadius: '14px',
                  background: 'rgba(255, 255, 255, 0.9)',
                  border: '1px solid rgba(0, 169, 206, 0.3)',
                  color: 'text.primary',
                  backdropFilter: 'blur(10px)',
                  transition: 'all 0.2s ease-in-out',
                  '&:hover': {
                    background: 'rgba(0, 169, 206, 0.1)',
                    transform: 'translateY(-1px)',
                    boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                  }
                }}
              />
            </Box>
          </Box>
        </Box>
      </CardContent>
      </Card>


    </Box>
  );
};