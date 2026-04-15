import React from 'react'
import { Link, useLocation } from 'react-router-dom'
import { FaBars, FaXmark, FaMagnifyingGlass, FaArrowUp, FaChartColumn } from 'react-icons/fa6'

export function Navbar() {
  const [isMenuOpen, setIsMenuOpen] = React.useState(false)
  const location = useLocation()

  const isActive = (path) => {
    return location.pathname === path || location.pathname.startsWith(path + '/')
      ? 'text-blue-400 border-b-2 border-blue-400'
      : 'text-slate-300 hover:text-white'
  }

  const navLinks = [
    { label: 'Dashboard', path: '/', icon: FaArrowUp },
    { label: 'Insights', path: '/insights', icon: FaChartColumn },
    { label: 'Job Search', path: '/jobs', icon: FaMagnifyingGlass },
  ]

  return (
    <nav className="bg-slate-800 border-b border-slate-700 sticky top-0 z-40">
      <div className="max-w-7xl mx-auto px-4 md:px-6">
        <div className="flex items-center justify-between h-16">
          {/* Logo */}
          <Link to="/" className="flex items-center gap-2 hover:opacity-80 transition">
            <div className="w-8 h-8 bg-gradient-to-br from-blue-600 to-blue-400 rounded-lg flex items-center justify-center text-white font-bold text-lg">
              JP
            </div>
            <span className="text-xl font-bold text-white hidden sm:inline">JobPulse</span>
          </Link>

          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center gap-8">
            {navLinks.map((link) => (
              <Link
                key={link.path}
                to={link.path}
                className={`pb-2 border-b-2 transition flex items-center gap-2 ${isActive(link.path)}`}
              >
                <link.icon className="w-4 h-4" />
                {link.label}
              </Link>
            ))}
          </div>

          {/* Mobile Menu Button */}
          <button
            onClick={() => setIsMenuOpen(!isMenuOpen)}
            className="md:hidden text-slate-300 hover:text-white transition"
            aria-label="Toggle menu"
          >
            {isMenuOpen ? (
              <FaXmark className="w-6 h-6" />
            ) : (
              <FaBars className="w-6 h-6" />
            )}
          </button>
        </div>

        {/* Mobile Navigation */}
        {isMenuOpen && (
          <div className="md:hidden pb-3 sm:pb-4 space-y-1 sm:space-y-2 border-t border-slate-700">
            {navLinks.map((link) => (
              <Link
                key={link.path}
                to={link.path}
                onClick={() => setIsMenuOpen(false)}
                className={`flex items-center gap-2 px-3 sm:px-4 py-2 sm:py-3 rounded-lg transition text-sm sm:text-base ${
                  location.pathname === link.path
                    ? 'bg-blue-900 bg-opacity-30 text-blue-400 font-medium'
                    : 'text-slate-300 hover:bg-slate-700'
                }`}
              >
                <link.icon className="w-4 h-4" />
                {link.label}
              </Link>
            ))}
          </div>
        )}
      </div>
    </nav>
  )
}
