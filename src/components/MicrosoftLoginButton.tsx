import React, { useState } from 'react';
import { useMsal } from '@azure/msal-react';
import { Button, Box, Typography, Alert } from '@mui/material';
import { Microsoft } from '@mui/icons-material';
import { loginRequest, strictLoginRequest, isMicrosoftOAuthConfigured } from '../config/msalConfig';
import { apiService } from '../services/api';

interface MicrosoftLoginButtonProps {
  onLogin: (userInfo: any) => void;
  disabled?: boolean;
  /**
   * Authentication mode:
   * - 'select_account': Shows account picker (allows switching accounts)
   * - 'login': Forces fresh authentication every time (most secure)
   * - 'auto': Allows silent authentication if available (least secure)
   */
  authMode?: 'select_account' | 'login' | 'auto';
}

const MicrosoftLoginButton: React.FC<MicrosoftLoginButtonProps> = ({ 
  onLogin, 
  disabled = false,
  authMode = 'select_account' // Default to account selection
}) => {
  // Check if Microsoft OAuth is configured
  if (!isMicrosoftOAuthConfigured()) {
    return null; // Don't render if not configured
  }

  let msalContext;
  try {
    msalContext = useMsal();
  } catch (error) {
    console.error('MSAL context error:', error);
    return null; // Don't render if MSAL context is not available
  }

  const { instance, accounts } = msalContext;
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleMicrosoftLogin = async () => {
    setLoading(true);
    setError(null); // Clear any previous errors

    try {
      let response;

      // Handle different authentication modes
      if (authMode === 'auto' && accounts.length > 0) {
        // Try silent login first (least secure - allows automatic login)
        try {
          const silentRequest = {
            ...loginRequest,
            account: accounts[0],
          };
          response = await instance.acquireTokenSilent(silentRequest);
        } catch (silentError) {
          // Fall through to interactive login
        }
      }

      // If silent auth wasn't attempted or failed, use interactive login
      if (!response) {
        let interactiveRequest;
        
        if (authMode === 'login') {
          // Force fresh authentication (most secure)
          interactiveRequest = {
            ...strictLoginRequest,
          };
        } else {
          // Default: force account selection (balanced security)
          interactiveRequest = {
            ...loginRequest,
          };
        }

        response = await instance.loginPopup(interactiveRequest);
      }
      
      if (response.accessToken) {
        await authenticateWithBackend(response.accessToken);
      } else {
        throw new Error('No access token received from Microsoft');
      }

    } catch (error: any) {
      console.error('Microsoft login error:', error);
      
      const isCancellation = 
        error?.errorCode === 'user_cancelled' ||
        error?.errorMessage?.includes('user_cancelled') ||
        error?.message?.includes('user_cancelled') ||
        error?.message?.includes('User cancelled') ||
        error?.errorCode === 'access_denied' ||
        error?.message?.includes('User closed the popup') ||
        error?.message?.includes('Popup window closed');
      
      if (isCancellation) {
        return;
      }
      
      // Only show error for actual failures, not user cancellations
      let errorMessage = 'Microsoft login failed';
      if (error?.message) {
        errorMessage = error.message;
      } else if (error?.errorCode) {
        errorMessage = `Microsoft login failed: ${error.errorCode}`;
      }
      
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const authenticateWithBackend = async (accessToken: string) => {
    try {
      // Send Microsoft access token to our backend
      const response = await apiService.microsoftLogin({ access_token: accessToken });
      
      // Set token and user info in our API service
      apiService.setToken(response.access_token);
      
      // Get full user info including permissions
      const userInfo = await apiService.getCurrentUser();
      
      onLogin(userInfo);
      
    } catch (error: any) {
      console.error('❌ Backend authentication error:', error);
      throw new Error(error.message || 'Failed to authenticate with backend');
    }
  };

  const handleLogout = async () => {
    try {
      await instance.logoutPopup();
      apiService.logout();
    } catch (error) {
      console.error('Logout error:', error);
    }
  };

  // If user is already logged in with Microsoft, show logout option
  if (accounts.length > 0 && !loading) {
    return (
      <Box sx={{ textAlign: 'center', mt: 2 }}>
        <Typography variant="body2" sx={{ mb: 1, color: 'text.secondary' }}>
          Signed in as: {accounts[0].name || accounts[0].username}
        </Typography>
        <Button
          variant="outlined"
          onClick={handleLogout}
          size="small"
          sx={{ mb: 1 }}
        >
          Sign out of Microsoft
        </Button>
      </Box>
    );
  }

  return (
    <Box sx={{ width: '100%' }}>
      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}
      
      <Button
        variant="outlined"
        fullWidth
        onClick={handleMicrosoftLogin}
        disabled={disabled || loading}
        startIcon={<Microsoft />}
        sx={{
          mt: 2,
          py: 1.5,
          borderColor: '#0078d4',
          color: '#0078d4',
          '&:hover': {
            borderColor: '#106ebe',
            backgroundColor: '#f3f9fd',
          },
          '&:disabled': {
            borderColor: '#ccc',
            color: '#999',
          },
        }}
      >
        {loading ? 'Signing in...' : 'Sign in with Microsoft'}
      </Button>
    </Box>
  );
};

export default MicrosoftLoginButton;