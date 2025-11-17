// API service
import type { Region } from '../types/region';

const API_BASE_URL = 'http://localhost:8000';

export interface ChatMessage {
  message: string;
  user_id?: string;
  session_id?: string;
  region?: Region | null;
}

export interface ChatResponse {
  response: string;
  response_type?: string;
  suggestions?: string[];
  requires_confirmation: boolean;
  operation_data?: any;
  context?: any;
  structured_content?: any;  // For rich content rendering
}

// Job Log interfaces
export interface JobLogRecord {
  id: number;
  schema_name: string | null;
  job_type: string;
  table_name: string;
  status: 'SUCCESS' | 'FAILED' | 'IN_PROGRESS';
  source: 'SCRIPT' | 'CHATBOT';
  reason: string | null;
  records_affected: number;
  started_at: string | null;
  finished_at: string | null;
  duration_seconds: number | null;
}



// Authentication interfaces
export interface LoginRequest {
  username: string;
  password: string;
}

export interface MicrosoftLoginRequest {
  access_token: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
  user_info: {
    username: string;
    role: string;
    email?: string;
    display_name?: string;
    auth_provider?: string;
  };
}

export interface UserInfo {
  username: string;
  role: string;
  permissions: string[];
  email?: string;
  display_name?: string;
  auth_provider?: string;
}

export interface OAuthConfig {
  microsoft_enabled: boolean;
}

class ApiService {
  private token: string | null = null;
  private userInfo: UserInfo | null = null;
  private refreshPromise: Promise<string> | null = null;

  constructor() {
    try {
      this.token = localStorage.getItem('access_token');
      const storedUserInfo = localStorage.getItem('user_info');
      if (storedUserInfo) {
        try {
          this.userInfo = JSON.parse(storedUserInfo);
        } catch (error) {
          console.error('Failed to parse stored user info:', error);
          this.clearSession();
        }
      }
    } catch (error) {
      console.error('Failed to initialize from localStorage:', error);
      this.forceWipeStorage();
    }
  }

  setToken(token: string) {
    this.token = token;
    localStorage.setItem('access_token', token);
  }

  setUserInfo(userInfo: UserInfo) {
    this.userInfo = userInfo;
    localStorage.setItem('user_info', JSON.stringify(userInfo));
  }

  clearToken() {
    this.token = null;
    localStorage.removeItem('access_token');
  }

  clearUserInfo() {
    this.userInfo = null;
    localStorage.removeItem('user_info');
  }

  clearSession() {
    // Clear instance variables
    this.clearToken();
    this.clearUserInfo();
    this.refreshPromise = null;
    
    // Force clear localStorage entries (in case individual methods didn't work)
    localStorage.removeItem('access_token');
    localStorage.removeItem('user_info');
    
    // Clear any other potential auth-related keys
    const authKeys = Object.keys(localStorage).filter(key => 
      key.includes('token') || 
      key.includes('user') || 
      key.includes('auth') ||
      key.includes('msal') ||
      key.includes('microsoft')
    );
    
    authKeys.forEach(key => {
      localStorage.removeItem(key);
    });
  }

  forceWipeStorage() {    
    // Reset instance variables
    this.token = null;
    this.userInfo = null;
    this.refreshPromise = null;
    
    // Nuclear option - clear everything
    try {
      localStorage.clear();
    } catch (error) {
      console.error('Failed to clear localStorage:', error);
      
      // Manual removal as fallback
      const keys = Object.keys(localStorage);
      keys.forEach(key => {
        try {
          localStorage.removeItem(key);
        } catch (e) {
          console.error(`Failed to remove key ${key}:`, e);
        }
      });
    }
  }

  getUserInfo(): UserInfo | null {
    return this.userInfo;
  }

  private async request<T>(
    endpoint: string, 
    options: RequestInit = {},
    retry: boolean = true
  ): Promise<T> {
    const url = `${API_BASE_URL}${endpoint}`;
    const headers: HeadersInit = {
      'Content-Type': 'application/json',
    };

    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    const config: RequestInit = {
      headers,
      ...options,
    };

    try {
      const response = await fetch(url, config);
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        
        if (response.status === 401 && retry) {
          // Try to refresh the token
          try {
            await this.refreshToken();
            // Retry the original request with the new token
            return this.request<T>(endpoint, options, false);
          } catch (refreshError) {
            this.clearSession();
            throw new Error('Session expired. Please log in again.');
          }
        } else if (response.status === 401) {
          this.clearSession();
          throw new Error('Authentication required. Please log in.');
        }
        
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      
      return await response.json();
    } catch (error) {
      if (error instanceof Error && error.message.includes('Failed to fetch')) {
        throw new Error('Unable to connect to server. Please check your connection.');
      }
      throw error;
    }
  }

  async login(credentials: LoginRequest): Promise<LoginResponse> {
    const response = await this.request<LoginResponse>('/auth/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    }, false); // Don't retry login requests
    
    this.setToken(response.access_token);
    this.setUserInfo({
      username: response.user_info.username,
      role: response.user_info.role,
      email: response.user_info.email,
      display_name: response.user_info.display_name,
      auth_provider: response.user_info.auth_provider || 'traditional',
      permissions: [] // Will be populated when fetching current user
    });
    
    return response;
  }

  async microsoftLogin(credentials: MicrosoftLoginRequest): Promise<LoginResponse> {
    const response = await this.request<LoginResponse>('/auth/microsoft/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    }, false); // Don't retry login requests
    
    this.setToken(response.access_token);
    this.setUserInfo({
      username: response.user_info.username,
      role: response.user_info.role,
      email: response.user_info.email,
      display_name: response.user_info.display_name,
      auth_provider: response.user_info.auth_provider || 'microsoft',
      permissions: [] // Will be populated when fetching current user
    });
    
    return response;
  }

  async getOAuthConfig(): Promise<OAuthConfig> {
    return this.request<OAuthConfig>('/auth/oauth/config', {
      method: 'GET',
    }, false);
  }

  async refreshToken(): Promise<string> {
    if (!this.token) {
      throw new Error('No token to refresh');
    }

    // Prevent multiple concurrent refresh requests
    if (this.refreshPromise) {
      return this.refreshPromise;
    }

    this.refreshPromise = (async () => {
      try {
        const response = await fetch(`${API_BASE_URL}/auth/refresh`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${this.token}`,
          },
        });

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          console.error('Token refresh failed:', response.status, errorData);
          throw new Error(`Token refresh failed: ${response.status}`);
        }

        const data: LoginResponse = await response.json();
        this.setToken(data.access_token);
        this.setUserInfo({
          username: data.user_info.username,
          role: data.user_info.role,
          permissions: []
        });
        
        return data.access_token;
      } catch (error) {
        console.error('Token refresh error:', error);
        throw error;
      } finally {
        this.refreshPromise = null;
      }
    })();

    return this.refreshPromise;
  }

  async getCurrentUser(): Promise<UserInfo> {
    if (!this.token) {
      throw new Error('No authentication token');
    }
    
    const userInfo = await this.request<UserInfo>('/auth/me');
    this.setUserInfo(userInfo);
    return userInfo;
  }

  logout() {
    this.clearSession();
    
    // Double-check that localStorage is actually clear
    setTimeout(() => {
      const remainingToken = localStorage.getItem('access_token');
      const remainingUser = localStorage.getItem('user_info');
      
      if (remainingToken || remainingUser) {
        console.warn('LocalStorage not fully cleared, forcing wipe...');
        this.forceWipeStorage();
      } else {
      }
    }, 100);
  }

  isAuthenticated(): boolean {
    return !!this.token && !!this.userInfo;
  }

  // Method to check if we have basic auth info (for initial load)
  hasStoredAuth(): boolean {
    return !!this.token;
  }

  // Method to decode JWT token and check expiry (client-side check)
  private isTokenExpired(): boolean {
    if (!this.token) return true;
    
    try {
      // Simple JWT decode (just the payload, not verifying signature)
      const parts = this.token.split('.');
      if (parts.length !== 3) return true;
      
      const payload = JSON.parse(atob(parts[1]));
      const currentTime = Math.floor(Date.now() / 1000);
      
      // Add 5 minute buffer before expiry to trigger refresh
      return payload.exp < (currentTime + 300);
    } catch (error) {
      console.error('Error decoding token:', error);
      return true;
    }
  }

  // Method to validate and potentially refresh token
  async validateSession(): Promise<boolean> {
    if (!this.token) return false;
    
    try {
      // If token is expired or about to expire, try to refresh
      if (this.isTokenExpired()) {
        await this.refreshToken();
      }
      
      // Verify the token is still valid with backend
      await this.getCurrentUser();
      return true;
    } catch (error) {
      console.error('Session validation failed:', error);
      this.clearSession();
      return false;
    }
  }

  // Chat with agent
  async chatWithAgent(message: ChatMessage): Promise<ChatResponse> {
    return this.request('/chat', {
      method: 'POST',
      body: JSON.stringify(message),
    });
  }

  async confirmChatOperation(confirmationId: string): Promise<ChatResponse> {
    return this.request('/chat/confirm', {
      method: 'POST',
      body: JSON.stringify({ confirmation_id: confirmationId }),
    });
  }

  // Confirm archive/delete operations with structured data
  async confirmOperation(confirmation: {
    operation: string;
    table: string;
    region: string;
    filters: any;
    confirmed: boolean;
  }): Promise<ChatResponse> {
    return this.request('/chat/confirm', {
      method: 'POST',
      body: JSON.stringify(confirmation),
    });
  }

  // Region management
  async getAvailableRegions(): Promise<string[]> {
    const response = await this.request<{regions: string[], connection_status: Record<string, boolean>}>('/regions/');
    return response.regions;
  }

  async connectToRegion(region: Region): Promise<{ success: boolean; message: string }> {
    return this.request('/regions/connect', {
      method: 'POST',
      body: JSON.stringify({ region }),
    });
  }

  async disconnectFromRegion(region: Region): Promise<{ success: boolean; message: string }> {
    return this.request('/regions/disconnect', {
      method: 'POST',
      body: JSON.stringify({ region }),
    });
  }

  async getRegionStatus(): Promise<Record<string, boolean>> {
    const response = await this.request<{regions: Record<string, boolean>, available_regions: string[]}>('/regions/status');
    return response.regions;
  }

  // Job Logs
  async getLatestErrorJobs(limit: number = 3): Promise<{
    success: boolean;
    records: JobLogRecord[];
    total_errors: number;
  }> {
    return this.request(`/job-logs/latest-errors?limit=${limit}`);
  }

  async getJobDetail(jobId: number): Promise<{
    success: boolean;
    record: JobLogRecord;
  }> {
    return this.request(`/job-logs/detail/${jobId}`);
  }

}

export const apiService = new ApiService();