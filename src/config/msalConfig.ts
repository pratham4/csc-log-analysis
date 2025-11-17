// Microsoft Authentication Library (MSAL) configuration

// MSAL configuration
export const msalConfig = {
  auth: {
    clientId: import.meta.env.VITE_AZURE_CLIENT_ID || '', // Your Azure App Registration Client ID
    authority: `https://login.microsoftonline.com/${import.meta.env.VITE_AZURE_TENANT_ID || 'common'}`,
    redirectUri: import.meta.env.VITE_AZURE_REDIRECT_URI || window.location.origin,
    postLogoutRedirectUri: import.meta.env.VITE_AZURE_POST_LOGOUT_REDIRECT_URI || window.location.origin,
  },
  cache: {
    cacheLocation: 'localStorage', // This configures where your cache will be stored
    storeAuthStateInCookie: false, // Set this to "true" if you are having issues on IE11 or Edge
  },
  system: {
    loggerOptions: {
      loggerCallback: (level: any, message: string, containsPii: boolean) => {
        if (containsPii) {
          return;
        }
        switch (level) {
          case 0: // LogLevel.Error
            console.error(message);
            return;
          case 1: // LogLevel.Warning
            console.warn(message);
            return;
          case 2: // LogLevel.Info
            return;
          case 3: // LogLevel.Verbose
            return;
          default:
            return;
        }
      },
    },
  },
};

// Scopes for Microsoft Graph API
export const loginRequest = {
  scopes: ['User.Read', 'email', 'profile'],
  prompt: 'select_account', // Force account selection every time
};

// For even stricter authentication, use this configuration
export const strictLoginRequest = {
  scopes: ['User.Read', 'email', 'profile'],
  prompt: 'login', // Force fresh authentication every time (most secure)
};

// Silent request configuration
export const silentRequest = {
  scopes: ['User.Read', 'email', 'profile'],
  forceRefresh: false, // Set this to "true" to skip a cached token and go to the server to get a new token
};

// Microsoft Graph API endpoints
export const graphConfig = {
  graphMeEndpoint: 'https://graph.microsoft.com/v1.0/me',
  graphUsersEndpoint: 'https://graph.microsoft.com/v1.0/users',
};

// Check if Microsoft OAuth is configured
export const isMicrosoftOAuthConfigured = (): boolean => {
  return !!(import.meta.env.VITE_AZURE_CLIENT_ID && import.meta.env.VITE_AZURE_TENANT_ID);
};