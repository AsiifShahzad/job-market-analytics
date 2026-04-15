import React from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ComposedChart,
  Bar,
} from 'recharts'
import { useSkills, useSkillTrend, useSkillCooccurrence } from '@/api/hooks/useSkills.js'
import { useFilterStore } from '@/stores/filterStore.js'
import { Sidebar } from '@/components/Sidebar.jsx'
import { categoryColors, getCategoryBgColor, getCategoryTextColor } from '@/utils/chartColors.js'
import { formatCurrency, formatTfIdfScore } from '@/utils/formatters.js'
import { FaSpinner, FaXmark, FaChevronDown } from 'react-icons/fa6'

export default function SkillsExplorer() {
  const [selectedSkill, setSelectedSkill] = React.useState(null)
  const [mobileFilterOpen, setMobileFilterOpen] = React.useState(false)

  const filters = useFilterStore()
  const { data: skillsData, isLoading } = useSkills({
    city: filters.city ?? undefined,
    country: filters.country ?? undefined,
    seniority: filters.seniority ?? undefined,
    category: filters.category ?? undefined,
    limit: 100,
  })

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="flex flex-col md:flex-row gap-6 max-w-7xl mx-auto">
        {/* Sidebar */}
        <div className="hidden md:block">
          <Sidebar />
        </div>

        {/* Mobile Sidebar Drawer */}
        {mobileFilterOpen && (
          <div className="md:hidden fixed inset-0 z-40 bg-black bg-opacity-50">
            <div className="bg-white h-full overflow-y-auto">
              <Sidebar onClose={() => setMobileFilterOpen(false)} />
            </div>
          </div>
        )}

        {/* Main Content */}
        <div className="flex-1 px-3 sm:px-4 md:px-0 py-4 sm:py-6\">
          {/* Header */}
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 sm:gap-0 mb-4 sm:mb-6\">\n            <h1 className="text-xl sm:text-2xl md:text-3xl font-bold text-gray-900\">Skills Explorer</h1>
            <button
              onClick={() => setMobileFilterOpen(true)}
              className="md:hidden flex items-center gap-2 px-3 sm:px-4 py-2 border border-gray-300 rounded-lg text-sm sm:text-base text-gray-700 hover:bg-gray-50"
            >
              <FaChevronDown className="w-4 h-4" />
              Filters
            </button>
          </div>

          {/* Skills Grid */}
          {isLoading ? (
            <div className="flex justify-center py-8 sm:py-12">
              <FaSpinner className="w-6 sm:w-8 h-6 sm:h-8 animate-spin text-gray-400\" />
            </div>
          ) : (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {(skillsData?.skills || []).map((skill) => (
                <SkillCard
                  key={skill.id}
                  skill={skill}
                  isSelected={selectedSkill === skill.name}
                  onSelect={() => setSelectedSkill(skill.name)}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Skill Detail Drawer */}
      {selectedSkill && (
        <SkillDetailDrawer
          skillName={selectedSkill}
          onClose={() => setSelectedSkill(null)}
        />
      )}
    </div>
  )
}

function SkillCard({ skill, isSelected, onSelect }) {
  return (
    <div
      onClick={onSelect}
      className={`p-4 rounded-lg border cursor-pointer transition-all ${
        isSelected
          ? 'border-purple-500 bg-purple-50 shadow-lg'
          : 'border-gray-200 bg-white hover:shadow-md'
      }`}
    >
      <div className="flex items-start justify-between mb-3">
        <div>
          <h3 className="font-semibold text-gray-900">{skill.name}</h3>
          <span className={`inline-block mt-1 text-xs font-medium px-2 py-1 rounded ${getCategoryBgColor(skill.category)} ${getCategoryTextColor(skill.category)}`}>
            {skill.category}
          </span>
        </div>
        <span className="inline-block bg-purple-100 text-purple-700 px-2 py-1 rounded text-xs font-medium">
          TF-IDF {formatTfIdfScore(skill.tfidf_score)}
        </span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-sm">
        <div>
          <p className="text-gray-600">Jobs</p>
          <p className="font-semibold text-gray-900">{skill.job_count}</p>
        </div>
        <div>
          <p className="text-gray-600">Avg Salary</p>
          <p className="font-semibold text-gray-900">
            {skill.avg_salary ? formatCurrency(skill.avg_salary) : 'N/A'}
          </p>
        </div>
      </div>
    </div>
  )
}

function SkillDetailDrawer({ skillName, onClose }) {
  const { data: trendData, isLoading: trendLoading } = useSkillTrend(skillName)
  const { data: cooccurData, isLoading: cooccurLoading } = useSkillCooccurrence(skillName)

  return (
    <div className="fixed inset-0 z-40 overflow-hidden">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black bg-opacity-50 transition-opacity"
        onClick={onClose}
      />

      {/* Drawer */}
      <div className="absolute right-0 top-0 h-full w-full md:w-96 bg-white shadow-xl overflow-y-auto">
        {/* Header */}
        <div className="sticky top-0 bg-white border-b border-gray-200 p-4 flex items-center justify-between">
          <h2 className="font-semibold text-lg text-gray-900">{skillName}</h2>
          <button
            onClick={onClose}
            className="p-2 hover:bg-gray-100 rounded"
          >
            <FaXmark className="w-5 h-5" />
          </button>
        </div>

        <div className="p-4 space-y-6">
          {/* Trend Chart */}
          <div>
            <h3 className="font-semibold mb-3 text-gray-900">Trend Over Time</h3>
            {trendLoading ? (
              <div className="h-64 flex items-center justify-center">
                <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
              </div>
            ) : trendData?.trends && trendData.trends.length > 0 ? (
              <ResponsiveContainer width="100%" height={250}>
                <ComposedChart data={trendData.trends}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Legend />
                  <Bar yAxisId="left" dataKey="job_count" fill="#14b8a6" />
                  <Line yAxisId="right" type="monotone" dataKey="tfidf_score" stroke="#9333ea" />
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-gray-600 text-sm">No trend data available</p>
            )}
          </div>

          {/* Co-occurring Skills */}
          <div>
            <h3 className="font-semibold mb-3 text-gray-900">Often Paired With</h3>
            {cooccurLoading ? (
              <FaSpinner className="w-6 h-6 animate-spin text-gray-400" />
            ) : cooccurData?.cooccurring && cooccurData.cooccurring.length > 0 ? (
              <div className="flex flex-wrap gap-2">
                {cooccurData.cooccurring.slice(0, 5).map((item, idx) => (
                  <span
                    key={idx}
                    className="inline-block bg-purple-100 text-purple-700 px-3 py-1 rounded-full text-sm font-medium"
                  >
                    {item.skill_name}
                  </span>
                ))}
              </div>
            ) : (
              <p className="text-gray-600 text-sm">No co-occurrence data available</p>
            )}
          </div>

          {/* Salary Band */}
          {trendData?.salary_band && (
            <div>
              <h3 className="font-semibold mb-3 text-gray-900">Salary Range</h3>
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">P25 (25th percentile)</span>
                  <span className="font-semibold text-gray-900">
                    {trendData.salary_band.p25 ? formatCurrency(trendData.salary_band.p25) : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">P50 (Median)</span>
                  <span className="font-semibold text-gray-900">
                    {trendData.salary_band.p50 ? formatCurrency(trendData.salary_band.p50) : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">P75 (75th percentile)</span>
                  <span className="font-semibold text-gray-900">
                    {trendData.salary_band.p75 ? formatCurrency(trendData.salary_band.p75) : 'N/A'}
                  </span>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
