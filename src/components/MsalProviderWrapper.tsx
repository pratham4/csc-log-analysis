import React from 'react';
import { MsalProvider } from '@azure/msal-react';
import { PublicClientApplication, EventType } from '@azure/msal-browser';
import type { AccountInfo } from '@azure/msal-browser';
import { msalConfig, isMicrosoftOAuthConfigured } from '../config/msalConfig';

// Create MSAL instance only if Microsoft OAuth is configured
let msalInstance: PublicClientApplication | null = null;

if (isMicrosoftOAuthConfigured()) {
  msalInstance = new PublicClientApplication(msalConfig);

  // Optional - This will update account state if a user signs in from another tab or window
  msalInstance.enableAccountStorageEvents();

  msalInstance.addEventCallback((event: any) => {
    if (event.eventType === EventType.LOGIN_SUCCESS && event.payload) {
      const account = event.payload.account as AccountInfo;
      msalInstance?.setActiveAccount(account);
    }
  });
}

interface MsalProviderWrapperProps {
  children: React.ReactNode;
}

const MsalProviderWrapper: React.FC<MsalProviderWrapperProps> = ({ children }) => {
  // If Microsoft OAuth is not configured, render children without MSAL provider
  if (!msalInstance) {
    return <>{children}</>;
  }

  return (
    <MsalProvider instance={msalInstance}>
      {children}
    </MsalProvider>
  );
};

export default MsalProviderWrapper;
export { msalInstance, isMicrosoftOAuthConfigured };