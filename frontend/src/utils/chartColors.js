/**
 * Chart color mapping by skill category
 */
export const categoryColors = {
  language: '#9333ea', // purple
  framework: '#14b8a6', // teal
  cloud: '#f59e0b', // amber
  tool: '#6b7280', // gray
  data: '#3b82f6', // blue
  soft: '#ec4899', // pink
}

export const getCategoryColor = (category) => {
  return categoryColors[category.toLowerCase()] || '#6b7280'
}

export const getCategoryBgColor = (category) => {
  const colors = {
    language: 'bg-purple-100',
    framework: 'bg-teal-100',
    cloud: 'bg-amber-100',
    tool: 'bg-gray-100',
    data: 'bg-blue-100',
    soft: 'bg-pink-100',
  }
  return colors[category.toLowerCase()] || 'bg-gray-100'
}

export const getCategoryTextColor = (category) => {
  const colors = {
    language: 'text-purple-700',
    framework: 'text-teal-700',
    cloud: 'text-amber-700',
    tool: 'text-gray-700',
    data: 'text-blue-700',
    soft: 'text-pink-700',
  }
  return colors[category.toLowerCase()] || 'text-gray-700'
}

/**
 * Chart color palette for multi-line/multi-bar charts
 */
export const chartPalette = [
  '#9333ea', // purple
  '#14b8a6', // teal
  '#f59e0b', // amber
  '#3b82f6', // blue
  '#ec4899', // pink
  '#6b7280', // gray
  '#6366f1', // indigo
  '#10b981', // emerald
]
