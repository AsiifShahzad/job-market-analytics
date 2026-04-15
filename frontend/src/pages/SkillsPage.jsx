import React, { useMemo } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ScatterChart,
  Scatter,
} from 'recharts'
import { useSkillTrend, useSkillCooccurrence } from '@/api/hooks/useSkills'
import { useJobsWithSkillsMatch } from '@/api/hooks/useJobs'
import { useFilterStore } from '@/stores/filterStore'
import { formatCurrency, formatNumber, formatDate } from '@/utils/formatters'
import { FaArrowLeft, FaArrowUp, FaBriefcase, FaDollarSign, FaBullseye } from 'react-icons/fa6'

export default function SkillsPage() {
  const { skillName } = useParams()
  const navigate = useNavigate()
  const store = useFilterStore()

  const { data: trendData, isLoading: trendLoading } = useSkillTrend(skillName)
  const { data: cooccurrenceData, isLoading: cooccurrenceLoading } =
    useSkillCooccurrence(skillName)

  // Fetch jobs that require this skill
  const filters = useMemo(
    () => ({
      ...store.getFilterParams(),
      skills: [skillName],
      skill_match_type: 'any',
    }),
    [skillName, store]
  )
  const { data: jobsData, isLoading: jobsLoading } = useJobsWithSkillsMatch([skillName])

  // Process data
  const trend = useMemo(() => {
    if (!trendData?.trend) return []
    return trendData.trend.map((item) => ({
      month: item.month,
      demand: item.demand,
      frequency: item.frequency,
    }))
  }, [trendData])

  const cooccurrence = useMemo(() => {
    if (!cooccurrenceData?.cooccurring_skills) return []
    return cooccurrenceData.cooccurring_skills
      .slice(0, 10)
      .map((item) => ({
        name: item.skill,
        frequency: item.frequency,
      }))
  }, [cooccurrenceData])

  const jobs = jobsData?.data || []
  const totalJobs = jobsData?.pagination?.total || 0

  // Calculate statistics
  const stats = useMemo(
    () => ({
      averageSalary: trendData?.average_salary || 0,
      medianSalary: trendData?.median_salary || 0,
      maxSalary: trendData?.max_salary || 0,
      minSalary: trendData?.min_salary || 0,
      jobCount: totalJobs,
      trend: trendData?.trend_direction || 'stable',
      demandIndex: trendData?.demand_index || 0,
    }),
    [trendData, totalJobs]
  )

  if (trendLoading) {
    return (
      <div className="min-h-screen bg-slate-900 flex items-center justify-center">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-slate-700 border-t-blue-400 rounded-full animate-spin mx-auto" />
          <p className="mt-4 text-slate-300">Loading skill details...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-slate-900 px-3 sm:px-4 md:px-6 py-4 sm:py-6 md:py-8">
      <div className="max-w-7xl mx-auto space-y-6 sm:space-y-8">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div>
            <button
              onClick={() => navigate(-1)}
              className="inline-flex items-center gap-2 text-blue-400 hover:text-blue-300 mb-4"
            >
              <FaArrowLeft className="w-4 h-4" />
              Back
            </button>
            <h1 className="text-4xl font-bold text-white">{skillName}</h1>
            <p className="text-slate-300 mt-2">Detailed market analysis and job opportunities</p>
          </div>
        </div>

        {/* Statistics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <StatCard
            title="Average Salary"
            value={formatCurrency(stats.averageSalary)}
            icon={<FaDollarSign className="w-6 h-6 text-green-400" />}
            color="bg-green-900 border-green-700"
          />
          <StatCard
            title="Jobs Available"
            value={formatNumber(stats.jobCount)}
            icon={<FaBriefcase className="w-6 h-6 text-blue-400" />}
            color="bg-blue-900 border-blue-700"
          />
          <StatCard
            title="Demand Index"
            value={stats.demandIndex.toFixed(1)}
            icon={<FaBullseye className="w-6 h-6 text-purple-400" />}
            color="bg-purple-900 border-purple-700"
          />
          <StatCard
            title="Trend"
            value={stats.trend.charAt(0).toUpperCase() + stats.trend.slice(1)}
            icon={<FaArrowUp className="w-6 h-6 text-orange-400" />}
            color="bg-orange-900 border-orange-700"
          />
        </div>

        {/* Salary Range */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
          <h2 className="text-xl font-bold text-white mb-4">Salary Range</h2>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
            <div>
              <p className="text-sm text-slate-400 mb-1">Minimum</p>
              <p className="text-2xl font-bold text-white">
                {formatCurrency(stats.minSalary)}
              </p>
            </div>
            <div>
              <p className="text-sm text-slate-400 mb-1">Median</p>
              <p className="text-2xl font-bold text-white">
                {formatCurrency(stats.medianSalary)}
              </p>
            </div>
            <div>
              <p className="text-sm text-slate-400 mb-1">Average</p>
              <p className="text-2xl font-bold text-white">
                {formatCurrency(stats.averageSalary)}
              </p>
            </div>
            <div>
              <p className="text-sm text-slate-400 mb-1">Maximum</p>
              <p className="text-2xl font-bold text-white">
                {formatCurrency(stats.maxSalary)}
              </p>
            </div>
          </div>
        </div>

        {/* Trend Chart */}
        {trend.length > 0 && (
          <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
            <h2 className="text-xl font-bold text-white mb-4">Demand Trend</h2>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trend}>
                <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                <XAxis dataKey="month" stroke="#cbd5e1" />
                <YAxis stroke="#cbd5e1" />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#cbd5e1' }}
                />
                <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                <Line
                  type="monotone"
                  dataKey="demand"
                  stroke="#3b82f6"
                  name="Demand"
                  strokeWidth={2}
                />
                <Line
                  type="monotone"
                  dataKey="frequency"
                  stroke="#10b981"
                  name="Frequency"
                  strokeWidth={2}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Co-occurring Skills */}
        {cooccurrence.length > 0 && (
          <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
            <h2 className="text-xl font-bold text-white mb-4">
              Frequently Paired With
            </h2>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart
                data={cooccurrence}
                layout="vertical"
                margin={{ left: 100, right: 20 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                <XAxis type="number" stroke="#cbd5e1" />
                <YAxis dataKey="name" type="category" width={100} stroke="#cbd5e1" />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#cbd5e1' }}
                />
                <Bar dataKey="frequency" fill="#8b5cf6" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        )}

        {/* Jobs Section */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-xl font-bold text-white">
              Jobs Requiring {skillName}
            </h2>
            <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-900 text-blue-300">
              {formatNumber(totalJobs)} jobs
            </span>
          </div>

          {jobsLoading ? (
            <div className="text-center py-12">
              <div className="w-8 h-8 border-4 border-slate-700 border-t-blue-400 rounded-full animate-spin mx-auto" />
              <p className="mt-4 text-slate-300">Loading jobs...</p>
            </div>
          ) : jobs.length > 0 ? (
            <div className="space-y-4">
              {jobs.slice(0, 5).map((job, idx) => (
                <div
                  key={idx}
                  className="p-4 border border-slate-700 bg-slate-700 bg-opacity-50 rounded-lg hover:border-blue-500 hover:shadow-md transition cursor-pointer"
                >
                  <h3 className="font-semibold text-white">{job.title}</h3>
                  <p className="text-sm text-slate-400 mt-1">{job.company}</p>
                  <div className="flex items-center justify-between mt-3">
                    <div className="flex items-center gap-4 text-sm text-slate-400">
                      {job.location && <span>{job.location}</span>}
                      {job.seniority && <span>{job.seniority}</span>}
                    </div>
                    {job.salary && (
                      <span className="font-semibold text-white">
                        {formatCurrency(job.salary)}
                      </span>
                    )}
                  </div>
                </div>
              ))}

              {totalJobs > 5 && (
                <button className="w-full py-2 px-4 text-blue-400 hover:bg-blue-900 hover:bg-opacity-30 rounded-lg font-medium transition">
                  View all {totalJobs} jobs
                </button>
              )}
            </div>
          ) : (
            <div className="text-center py-12 text-slate-400">
              <FaBriefcase className="w-12 h-12 text-slate-500 mx-auto mb-2 opacity-50" />
              <p>No jobs found for this skill</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

/**
 * StatCard component
 */
function StatCard({ title, value, icon, color }) {
  return (
    <div className={`${color} bg-opacity-30 rounded-lg p-4 border`}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-sm font-medium text-slate-300">{title}</p>
          <p className="text-2xl font-bold text-white mt-1">{value}</p>
        </div>
        {icon}
      </div>
    </div>
  )
}
