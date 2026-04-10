import React from 'react'
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom'
import Dashboard from './pages/Dashboard.jsx'
import JobSearchPage from './pages/JobSearchPage.jsx'
import SkillsPage from './pages/SkillsPage.jsx'
import { Navbar } from './components/Navbar.jsx'

function App() {
  return (
    <Router future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <div className="min-h-screen bg-gray-50">
        <Navbar />
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/jobs" element={<JobSearchPage />} />
          <Route path="/skills/:skillName" element={<SkillsPage />} />
          {/* Add more routes here as you build more pages */}
        </Routes>
      </div>
    </Router>
  )
}

export default App
