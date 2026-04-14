import React from 'react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { useSalaries, useSkillPremium } from '@/api/hooks/useSalaries.js'
import { useFilterStore } from '@/stores/filterStore.js'
import { formatCurrency, formatPercent, formatGrowth } from '@/utils/formatters.js'
import { FaSpinner, FaArrowUp, FaArrowDown } from 'react-icons/fa6'

export default function SalaryInsights() {
  const [selectedTitle, setSelectedTitle] = React.useState('')
  const filters = useFilterStore()

  const { data: salariesData, isLoading: salariesLoading } = useSalaries({
    title: selectedTitle || undefined,
    city: filters.city ?? undefined,
  })

  const { data: premiumData, isLoading: premiumLoading } = useSkillPremium({
    city: filters.city ?? undefined,
  })

  const salaryRecords = salariesData?.data || []
  const uniqueTitles = [
    ...new Set(salaryRecords.map((s) => s.title)),
  ]

  return (
    <div className="min-h-screen bg-gray-50 px-4 md:px-6 py-8">
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Salary Insights</h1>
          <p className="text-gray-600">Average compensation by role title and skill premium analysis</p>
        </div>

        {/* Job Title Filter */}
        <div className="bg-white rounded-lg shadow p-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Filter by Job Title
          </label>
          <select
            value={selectedTitle}
            onChange={(e) => setSelectedTitle(e.target.value)}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-purple-500"
          >
            <option value="">All titles</option>
            {uniqueTitles.slice(0, 10).map((title) => (
              <option key={title} value={title}>
                {title}
              </option>
            ))}
          </select>
        </div>

        {/* Salary Chart */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold mb-4 text-gray-900">Salary Distribution</h2>
          {salariesLoading ? (
            <div className="h-80 flex items-center justify-center">
              <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
            </div>
          ) : salaryRecords.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <ComposedChart
                data={salaryRecords}
                margin={{ top: 20, right: 30, left: 20, bottom: 20 }}
              >
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="title" angle={-45} textAnchor="end" height={100} />
                <YAxis />
                <Tooltip formatter={(value) => formatCurrency(value)} />
                <Legend />
                {/* Range bar (p25 to p75) */}
                <Bar dataKey="p25" stackId="salary" fill="#e5e7eb" name="P25" />
                <Bar dataKey="p50" stackId="salary" fill="#3b82f6" name="Median" />
                <Bar dataKey="p75" stackId="salary" fill="#1e293b" name="P75" />
              </ComposedChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-gray-600 py-12 text-center">No salary data available</p>
          )}
        </div>

        {/* Skill Premium Table */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Positive Premium */}
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold mb-4 text-green-900">High Premium Skills</h3>
            {premiumLoading ? (
              <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 font-medium text-gray-700">Skill</th>
                      <th className="text-right py-2 font-medium text-gray-700">Premium</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(premiumData?.premiums || [])
                      .filter((p) => p.delta > 0)
                      .sort((a, b) => b.delta_pct - a.delta_pct)
                      .slice(0, 5)
                      .map((premium, idx) => (
                        <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 font-medium text-gray-900">{premium.skill}</td>
                          <td className="py-2 text-right">
                            <div className="flex items-center justify-end gap-1">
                              <FaArrowUp className="w-4 h-4 text-green-600" />
                              <span className="text-green-600 font-semibold">
                                +{formatGrowth(premium.delta_pct / 100)}
                              </span>
                            </div>
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Negative Premium */}
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold mb-4 text-red-900">Lower Premium Skills</h3>
            {premiumLoading ? (
              <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 font-medium text-gray-700">Skill</th>
                      <th className="text-right py-2 font-medium text-gray-700">Premium</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(premiumData?.premiums || [])
                      .filter((p) => p.delta < 0)
                      .sort((a, b) => a.delta_pct - b.delta_pct)
                      .slice(0, 5)
                      .map((premium, idx) => (
                        <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 font-medium text-gray-900">{premium.skill}</td>
                          <td className="py-2 text-right">
                            <div className="flex items-center justify-end gap-1">
                              <FaArrowDown className="w-4 h-4 text-red-600" />
                              <span className="text-red-600 font-semibold">
                                {formatGrowth(premium.delta_pct / 100)}
                              </span>
                            </div>
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Full Skill Premium Table */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold mb-4 text-gray-900">Complete Skill Premium Analysis</h2>
          {premiumLoading ? (
            <div className="flex justify-center py-12">
              <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200 bg-gray-50">
                    <th className="text-left py-3 px-4 font-semibold text-gray-700">Skill</th>
                    <th className="text-right py-3 px-4 font-semibold text-gray-700">With Skill</th>
                    <th className="text-right py-3 px-4 font-semibold text-gray-700">Without Skill</th>
                    <th className="text-right py-3 px-4 font-semibold text-gray-700">Delta</th>
                    <th className="text-right py-3 px-4 font-semibold text-gray-700">% Change</th>
                  </tr>
                </thead>
                <tbody>
                  {(premiumData?.premiums || []).map((premium, idx) => {
                    const isPositive = premium.delta_pct > 0
                    return (
                      <tr
                        key={idx}
                        className="border-b border-gray-100 hover:bg-gray-50"
                      >
                        <td className="py-2 px-4 font-medium text-gray-900">{premium.skill}</td>
                        <td className="py-2 px-4 text-right text-gray-900">
                          {formatCurrency(premium.avg_with_skill)}
                        </td>
                        <td className="py-2 px-4 text-right text-gray-900">
                          {formatCurrency(premium.avg_without_skill)}
                        </td>
                        <td className={`py-2 px-4 text-right font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                          {isPositive ? '+' : ''}{formatCurrency(premium.delta)}
                        </td>
                        <td className={`py-2 px-4 text-right font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                          {isPositive ? '+' : ''}{formatGrowth(premium.delta_pct / 100)}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
