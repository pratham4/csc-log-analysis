import { useState, useEffect } from 'react';
import {
  ThemeProvider,
  createTheme,
  CssBaseline,
  AppBar,
  Toolbar,
  Typography,
  Container,
  Box,
  Paper,
  Button,
  Menu,
  MenuItem,
} from '@mui/material';
import {
  KeyboardArrowDown as ArrowDownIcon,
  Logout as LogoutIcon,
} from '@mui/icons-material';
import type { Region } from './types/region';
import { ChatBot } from './components/ChatBot';
import RegionPanel from './components/RegionPanel';
import JobErrorsPanel from './components/JobErrorsPanel';
import Login from './components/Login';
import { apiService } from './services/api';
import type { UserInfo } from './services/api';
import { useMsal } from '@azure/msal-react';
import { isMicrosoftOAuthConfigured } from './config/msalConfig';

const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#1F4C5F',
    },
    background: {
      default: '#ffffff',
    },
  },
  typography: {
    fontFamily: '"Inter", "Segoe UI", "Roboto", sans-serif',
  },
  components: {
    MuiCssBaseline: {
      styleOverrides: {
        body: {
          background: '#ffffff',
          minHeight: '100vh',
        },
      },
    },
  },
});

function App() {
  const [userInfo, setUserInfo] = useState<UserInfo | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedRegion, setSelectedRegion] = useState<Region | null>(null);
  const [regionStatus, setRegionStatus] = useState<Record<string, boolean>>({});
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const open = Boolean(anchorEl);

  // MSAL instance for Microsoft logout (only if configured)
  let msalInstance = null;
  try {
    if (isMicrosoftOAuthConfigured()) {
      const msal = useMsal();
      msalInstance = msal.instance;
    }
  } catch (error) {
    // MSAL not available, continue without it
  }

  useEffect(() => {
    // Check if user is already logged in
    const checkAuth = async () => {
      try {
        // Check if we have stored authentication data
        if (apiService.hasStoredAuth()) {
          const isValid = await apiService.validateSession();
          
          if (isValid) {
            const userInfo = apiService.getUserInfo();
            if (userInfo) {
              setUserInfo(userInfo);
            }
          }
        } else {
          // Check if we have stored user info from previous session
          const storedUserInfo = apiService.getUserInfo();
          if (storedUserInfo) {
            // We have user info but no token, session expired
            apiService.clearSession();
          }
        }
      } catch (error) {
        console.error('Authentication check failed:', error);
        apiService.clearSession();
      } finally {
        setIsLoading(false);
      }
    };

    checkAuth();
  }, []);

  const handleLogin = (user: any) => {
    setUserInfo(user);
  };

  const handleLogout = async () => {
    try {
      // Clear local session
      apiService.logout();
      setUserInfo(null);
      setSelectedRegion(null);
      setRegionStatus({});
      setAnchorEl(null);
            
      localStorage.removeItem('access_token');
      localStorage.removeItem('user_info');
      
      // Clear any Microsoft MSAL localStorage entries
      const allKeys = Object.keys(localStorage);
      
      const msalKeys = allKeys.filter(key => {
        // Standard MSAL prefixes
        if (key.includes('msal') || key.includes('microsoft') || key.includes('azure')) {
          return true;
        }
        
        // Microsoft domain patterns
        if (key.includes('login.microsoftonline.com') || key.includes('login.windows.net')) {
          return true;
        }
        
        // UUID-based account keys pattern: UUID.UUID-domain-UUID
        const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[a-zA-Z0-9.-]+-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        if (uuidPattern.test(key)) {
          return true;
        }
        
        // Token cache patterns
        if (key.includes('AccessToken') || key.includes('RefreshToken') || 
            key.includes('IdToken') || key.includes('Account')) {
          return true;
        }
        
        return false;
      });
      
      msalKeys.forEach(key => {
        
        localStorage.removeItem(key);
      });

      // Content-based cleanup for any remaining Microsoft-related data
      
      const remainingKeys = Object.keys(localStorage);
      remainingKeys.forEach(key => {
        try {
          const value = localStorage.getItem(key);
          if (value && typeof value === 'string') {
            try {
              // Try to parse as JSON to check for MSAL-like structures
              const parsed = JSON.parse(value);
              if (parsed && typeof parsed === 'object') {
                // Check for MSAL-specific properties
                const msalProperties = [
                  'authorityType', 'clientInfo', 'homeAccountId', 
                  'tenantProfiles', 'environment', 'realm', 
                  'localAccountId', 'lastUpdatedAt'
                ];
                
                const hasMsalProps = msalProperties.some(prop => 
                  parsed.hasOwnProperty(prop)
                );
                
                // Check for Microsoft domains in the content
                const hasMicrosoftDomains = [
                  'login.windows.net', 'login.microsoftonline.com',
                  'microsoft.com', 'azure.com'
                ].some(domain => value.includes(domain));
                
                if (hasMsalProps || hasMicrosoftDomains) {
                  
                  localStorage.removeItem(key);
                }
              }
            } catch (parseError) {
              // If it's not JSON, check string content for Microsoft patterns
              const microsoftPatterns = [
                'login.windows.net', 'login.microsoftonline.com',
                'microsoft.com', 'azure.com', 'MSSTS'
              ];
              
              if (microsoftPatterns.some(pattern => value.includes(pattern))) {
                
                localStorage.removeItem(key);
              }
            }
          }
        } catch (error) {
          console.warn('Error checking key:', key, error);
        }
      });
      

      
      // If user was logged in via Microsoft OAuth, also logout from Microsoft
      if (userInfo?.auth_provider === 'microsoft' && msalInstance) {
        
        
        // Clear MSAL cache before logout
        try {
          const accounts = msalInstance.getAllAccounts();
          
          // Clear cache for each account individually using proper MSAL API
          for (const account of accounts) {
            try {
              await msalInstance.clearCache({ account });
              
            } catch (cacheError) {
              console.warn('⚠️ Failed to clear cache for account:', account.username || account.homeAccountId, cacheError);
            }
          }
          
          // Also try global cache clear
          await msalInstance.clearCache();
          
          
          // Logout from Microsoft
          if (accounts.length > 0) {
            await msalInstance.logoutPopup({
              account: accounts[0],
              postLogoutRedirectUri: window.location.origin
            });
          }
        } catch (error) {
          console.warn('⚠️ Failed to clear MSAL cache:', error);
        }
        
        
      }

      // Final comprehensive cleanup - scan for any remaining auth-related data
      
      const finalKeys = Object.keys(localStorage);
      finalKeys.forEach(key => {
        // Remove any remaining keys that look like MSAL account identifiers
        // Pattern: UUID.UUID-domain-UUID or similar variations
        const msalAccountPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(\.[0-9a-f-]+)*-(login\.)?(windows\.net|microsoftonline\.com|azure\.com)/i;
        
        if (msalAccountPattern.test(key)) {
          
          localStorage.removeItem(key);
        }
      });
      

      
      // Final verification - check if any auth data remains
      const remainingToken = localStorage.getItem('access_token');
      const remainingUser = localStorage.getItem('user_info');
      
      if (remainingToken || remainingUser) {
        console.warn('⚠️ App: Some auth data still present, forcing nuclear clear...');
        localStorage.clear();
      }
      
      
      
      // Force page reload to ensure clean state
      window.location.reload();
      
    } catch (error) {
      console.error('❌ Logout error:', error);
      
      // Fallback: clear everything and reload
      apiService.logout();
      setUserInfo(null);
      setSelectedRegion(null);
      setRegionStatus({});
      setAnchorEl(null);
      
      // Nuclear option - clear all localStorage
      
      localStorage.clear();
      
      // Force reload even on error
      window.location.reload();
    }
  };

  const handleMenuClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleMenuClose = () => {
    setAnchorEl(null);
  };

  const handleRegionStatusChange = (status: Record<string, boolean>) => {
    setRegionStatus(status);
  };

  if (isLoading) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box
          sx={{
            minHeight: '100vh',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: '#ffffff',
          }}
        >
          <Typography variant="h6" color="#1F4C5F">
            Loading...
          </Typography>
        </Box>
      </ThemeProvider>
    );
  }

  if (!userInfo) {
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Login onLogin={handleLogin} />
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box
          sx={{
            minHeight: '100vh',
            background: '#ffffff',
          }}
        >
          {/* Header */}
          <AppBar 
            position="static" 
            elevation={0}
            sx={{ 
              background: '#253746',
              boxShadow: '0 2px 10px rgba(31, 76, 95, 0.1)',
            }}
          >
            <Toolbar sx={{ py: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', flexGrow: 1 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                  <img 
                    src="/DSI_logo.png" 
                    alt="DSI Logo" 
                    style={{ width: 32, height: 32, borderRadius: '6px' }}
                  />
                  <Typography 
                    variant="h6" 
                    sx={{ 
                      fontWeight: 700, 
                      color: 'white',
                      fontSize: '1.1rem'
                    }}
                  >
                    Cloud Inventory Assistant
                  </Typography>
                </Box>
              </Box>
              
              {/* User Info with Dropdown */}
              <Box sx={{ display: 'flex', alignItems: 'center' }}>
                <Button
                  color="inherit"
                  onClick={handleMenuClick}
                  endIcon={<ArrowDownIcon />}
                  sx={{
                    background: '#253746',
                    fontSize: '0.875rem',
                    px: 2,
                    py: 1,
                    borderRadius: '8px',
                    textTransform: 'none'
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ 
                      width: 48, 
                      height: 48,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center'
                    }}>
                      <img 
                        src="/user_logo.svg" 
                        alt="User" 
                        style={{ width: 48, height: 48 }}
                      />
                    </Box>
                    <Box sx={{ textAlign: 'left' }}>
                      <Typography variant="body2" sx={{ fontWeight: 600, fontSize: '0.875rem', lineHeight: 1, color: 'white' }}>
                        {userInfo.display_name || userInfo.username.charAt(0).toUpperCase() + userInfo.username.slice(1)}
                      </Typography>
                      <Box sx={{ 
                        backgroundColor: '#1F4C5F', 
                        borderRadius: '12px', 
                        px: 1, 
                        py: 0.25, 
                        mt: 0.5,
                        display: 'inline-block'
                      }}>
                        <Typography variant="caption" sx={{ color: 'white', fontSize: '0.65rem', fontWeight: 500 }}>
                          {userInfo.role.charAt(0).toUpperCase() + userInfo.role.slice(1)}
                          {userInfo.auth_provider === 'microsoft' && ' (Microsoft)'}
                        </Typography>
                      </Box>
                    </Box>
                  </Box>
                </Button>
                
                <Menu
                  anchorEl={anchorEl}
                  open={open}
                  onClose={handleMenuClose}
                  sx={{
                    '& .MuiPaper-root': {
                      backgroundColor: 'white',
                      boxShadow: '0 8px 32px rgba(0, 0, 0, 0.12)',
                      borderRadius: '8px',
                      minWidth: '200px',
                      mt: 1
                    }
                  }}
                  transformOrigin={{ horizontal: 'right', vertical: 'top' }}
                  anchorOrigin={{ horizontal: 'right', vertical: 'bottom' }}
                >
                  <MenuItem 
                    onClick={handleLogout}
                    sx={{ 
                      fontSize: '0.875rem',
                      py: 1.5,
                      px: 2,
                      display: 'flex',
                      justifyContent: 'center',
                      alignItems: 'center',
                      '&:hover': {
                        backgroundColor: 'rgba(239, 68, 68, 0.1)'
                      }
                    }}
                  >
                    <LogoutIcon sx={{ fontSize: 16, mr: 1, color: '#ef4444' }} />
                    Logout
                  </MenuItem>
                </Menu>
              </Box>
            </Toolbar>
          </AppBar>

          {/* Main Container */}
          <Container maxWidth="xl" sx={{ py: 3 }}>
            <Box sx={{ 
              display: 'flex', 
              flexDirection: { xs: 'column', lg: 'row' },
              gap: 3,
              height: 'calc(100vh - 120px)' // Adjust based on header height
            }}>
              {/* Chat Section */}
              <Box sx={{ flex: 3, height: '100%' }}>
                <Paper 
                  elevation={2}
                  sx={{
                    background: '#ffffff',
                    borderRadius: '12px',
                    overflow: 'hidden',
                    boxShadow: '0 4px 20px rgba(0, 0, 0, 0.08)',
                    height: '100%',
                    border: '1px solid #e5e7eb',
                  }}
                >
                  <Box sx={{ height: '100%' }}>
                    <ChatBot 
                      userId={userInfo.username} 
                      userRole={userInfo.role}
                      selectedRegion={selectedRegion}
                      regionStatus={regionStatus}
                    />
                  </Box>
                </Paper>
              </Box>
              
              {/* Right Side Panel */}
              <Box sx={{ flex: 1, minWidth: 280, maxWidth: 320, height: '100%' }}>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 3, height: '100%' }}>
                  {/* Region Panel */}
                  <Box sx={{ flex: '0 0 auto' }}>
                    <RegionPanel
                      selectedRegion={selectedRegion}
                      onRegionChange={setSelectedRegion}
                      onRegionStatusChange={handleRegionStatusChange}
                    />
                  </Box>
                  
                  {/* Job Errors Panel */}
                  <Box sx={{ flex: '1 1 auto', minHeight: 0 }}>
                    <JobErrorsPanel selectedRegion={selectedRegion} />
                  </Box>
                </Box>
              </Box>
            </Box>
          </Container>
        </Box>
      </ThemeProvider>
  );
}

export default App;
