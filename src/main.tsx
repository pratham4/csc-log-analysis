import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import MsalProviderWrapper from './components/MsalProviderWrapper'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MsalProviderWrapper>
      <App />
    </MsalProviderWrapper>
  </StrictMode>,
)
