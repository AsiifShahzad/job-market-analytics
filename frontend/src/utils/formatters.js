/**
 * Format number as currency
 */
export const formatCurrency = (value) => {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
}

/**
 * Format number with commas
 */
export const formatNumber = (value) => {
  return new Intl.NumberFormat('en-US').format(value)
}

/**
 * Format percentage
 */
export const formatPercent = (value, decimals = 1) => {
  return `${(value * 100).toFixed(decimals)}%`
}

/**
 * Format growth percentage with sign
 */
export const formatGrowth = (value, decimals = 1) => {
  const sign = value > 0 ? '+' : ''
  return `${sign}${(value * 100).toFixed(decimals)}%`
}

/**
 * Format TF-IDF score (0-1 range)
 */
export const formatTfIdfScore = (score) => {
  return score.toFixed(2)
}

/**
 * Format date to readable string
 */
export const formatDate = (dateString) => {
  const date = new Date(dateString)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })
}

/**
 * Format datetime
 */
export const formatDateTime = (dateString) => {
  const date = new Date(dateString)
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

/**
 * Format time ago (e.g., "2 hours ago")
 */
export const formatTimeAgo = (dateString) => {
  const date = new Date(dateString)
  const now = new Date()
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000)

  if (seconds < 60) return 'just now'
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`

  return formatDate(dateString)
}

/**
 * Capitalize first letter
 */
export const capitalize = (str) => {
  return str.charAt(0).toUpperCase() + str.slice(1)
}

/**
 * Format pipeline status for display
 */
export const formatPipelineStatus = (status) => {
  const statusMap = {
    SUCCESS: { label: 'Success', color: 'bg-green-100 text-green-800' },
    RUNNING: { label: 'Running', color: 'bg-blue-100 text-blue-800 animate-pulse-fast' },
    FAILED: { label: 'Failed', color: 'bg-red-100 text-red-800' },
    PENDING: { label: 'Pending', color: 'bg-yellow-100 text-yellow-800' },
  }
  return statusMap[status] || { label: status, color: 'bg-gray-100 text-gray-800' }
}
