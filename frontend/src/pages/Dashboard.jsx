import React, { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { useSkills } from '@/api/hooks/useSkills.js'
import { useEmergingSkills } from '@/api/hooks/useTrends.js'
import { usePipelineRuns } from '@/api/hooks/usePipeline.js'
import { formatNumber, formatDate, formatTfIdfScore } from '@/utils/formatters.js'
import { FaSpinner, FaArrowUp, FaHeart, FaArrowRight, FaMagnifyingGlass, FaExclamation } from 'react-icons/fa6'

export default function Dashboard() {
  const navigate = useNavigate()
  const { data: skillsData, isLoading: skillsLoading, error: skillsError } = useSkills()
  const { data: emergingData, isLoading: emergingLoading, error: emergingError } = useEmergingSkills()
  const { data: runsData, isLoading: runsLoading, error: runsError } = usePipelineRuns()

  // Strict: only use real API data — no mock fallbacks
  const skills = Array.isArray(skillsData?.skills) ? skillsData.skills : []
  const emerging = Array.isArray(emergingData?.emerging_skills) ? emergingData.emerging_skills : []
  const runs = Array.isArray(runsData?.runs) ? runsData.runs : []

  // Prepare chart data
  const chartData = useMemo(() => {
    const safeSkills = Array.isArray(skills) ? skills : []
    return safeSkills.slice(0, 10).map((skill) => ({
      name: skill.name || 'Unknown',
      frequency: skill.frequency || skill.demand || skill.jobs_count || 0,
      average_salary: skill.average_salary || skill.avg_salary || 0,
    }))
  }, [skills])

  const latestRun = (runs || [])[0]
  const totalJobs = latestRun?.jobs_inserted || 0
  const uniqueSkills = latestRun?.unique_skills || 0

  const handleSkillClick = (skillName) => {
    navigate(`/skills/${encodeURIComponent(skillName)}`)
  }

  const handleGoToJobs = () => {
    navigate('/jobs')
  }

  const handleSearchBySkill = (skillName) => {
    navigate(`/jobs?skills=${encodeURIComponent(skillName)}`)
  }

  return (
    <div className="min-h-screen bg-slate-900 px-3 sm:px-4 md:px-6 py-4 sm:py-6 md:py-8">
      <div className="max-w-7xl mx-auto space-y-6 sm:space-y-8">
        {/* Header */}
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-4 sm:gap-0">
          <div>
            <h1 className="text-2xl sm:text-3xl md:text-4xl font-bold text-white">Dashboard</h1>
            <p className="text-sm sm:text-base text-slate-300 mt-1 sm:mt-2">Monitor job market trends and skill demand</p>
            <div className="mt-3 inline-block px-3 py-1 bg-blue-900 bg-opacity-30 border border-blue-700 rounded-full text-xs font-medium text-blue-300">
              ✓ Analyzed from verified real job listings
            </div>
          </div>
          <button
            onClick={handleGoToJobs}
            className="hidden md:inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition font-medium"
          >
            <FaMagnifyingGlass className="w-4 h-4" />
            Search Jobs
          </button>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 sm:gap-4 md:gap-6">
          <MetricCard
            title="Total Jobs"
            value={formatNumber(totalJobs)}
            icon={<FaHeart className="w-6 h-6 text-blue-400" />}
            color="bg-blue-900 border-blue-700"
          />
          <MetricCard
            title="Unique Skills"
            value={formatNumber(uniqueSkills)}
            icon={<FaArrowUp className="w-6 h-6 text-purple-400" />}
            color="bg-purple-900 border-purple-700"
          />
          <MetricCard
            title="Last Update"
            value={latestRun?.completed_at ? formatDate(latestRun.completed_at) : 'No data'}
            icon={<FaHeart className="w-6 h-6 text-green-400" />}
            color="bg-green-900 border-green-700"
          />
        </div>

        {/* CTA Banner */}
        <div className="bg-gradient-to-r from-blue-600 to-blue-700 rounded-lg shadow-lg p-4 sm:p-6 md:p-8 text-white">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-2xl font-bold">Ready to Find Your Next Opportunity?</h2>
              <p className="text-blue-100 mt-2">
                Search for jobs by skills, location, salary, and more
              </p>
            </div>
            <button
              onClick={handleGoToJobs}
              className="hidden md:inline-flex items-center gap-2 px-6 py-3 bg-white text-blue-600 rounded-lg hover:bg-blue-50 transition font-semibold"
            >
              Start Searching
              <FaArrowRight className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Top Skills Chart */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-white mb-6">Top 10 In-Demand Skills</h2>
          {skillsLoading ? (
            <div className="flex items-center justify-center py-12">
              <FaSpinner className="w-6 h-6 animate-spin text-slate-400" />
              <span className="ml-2 text-slate-300">Loading skills...</span>
            </div>
          ) : skillsError ? (
            <div className="flex items-center justify-center gap-2 py-12 text-red-400">
              <FaExclamation className="w-5 h-5" />
              <span>API error: {skillsError?.message || 'Failed to load skills'}</span>
            </div>
          ) : chartData.length === 0 ? (
            <p className="text-center text-slate-400 py-12">No skills data — run the pipeline to fetch job data</p>
          ) : (
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                <XAxis dataKey="name" stroke="#cbd5e1" angle={-45} textAnchor="end" height={100} />
                <YAxis stroke="#cbd5e1" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                  labelStyle={{ color: '#cbd5e1' }}
                  formatter={(value) => formatNumber(value)}
                />
                <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                <Bar dataKey="frequency" fill="#a855f7" name="Job Count" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Emerging Skills */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-white mb-6">Emerging Skills (Recent Trend)</h2>
          {emergingLoading ? (
            <div className="flex items-center justify-center py-12">
              <FaSpinner className="w-6 h-6 animate-spin text-slate-400" />
              <span className="ml-2 text-slate-300">Loading trends...</span>
            </div>
          ) : emergingError ? (
            <div className="flex items-center justify-center gap-2 py-12 text-red-400">
              <FaExclamation className="w-5 h-5" />
              <span>API error: {emergingError?.message || 'Failed to load trends'}</span>
            </div>
          ) : emerging.length === 0 ? (
            <p className="text-center text-slate-400 py-12">No emerging skills data — run the pipeline first to collect job data</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {Array.isArray(emerging) && emerging.slice(0, 6).map((skill) => (
                <div key={skill.name} className="p-4 border border-slate-600 bg-slate-700 bg-opacity-50 rounded-lg hover:border-purple-500 transition">
                  <div className="flex items-center justify-between mb-2">
                    <button
                      onClick={() => handleSkillClick(skill.name)}
                      className="font-semibold text-white hover:text-purple-400 transition"
                    >
                      {skill.name}
                    </button>
                    <FaArrowUp className="w-5 h-5 text-green-400" />
                  </div>
                  {skill.growth_rate > 0 ? (
                    <p className="text-sm text-slate-300 mb-3">
                      Growth: <span className="font-semibold text-green-400">{formatTfIdfScore(skill.growth_rate)}</span>
                    </p>
                  ) : null}
                  <p className="text-xs text-slate-400 mb-3">
                    Jobs: {formatNumber(skill.job_count || 0)}
                  </p>
                  <button
                    onClick={() => handleSearchBySkill(skill.name)}
                    className="w-full text-xs px-3 py-1.5 border border-purple-600 text-purple-300 rounded hover:bg-purple-900 hover:bg-opacity-50 transition font-medium"
                  >
                    View Jobs
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Pipeline Runs */}
        <div className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-white mb-6">Recent Pipeline Runs</h2>
          {runsLoading ? (
            <div className="flex items-center justify-center py-12">
              <FaSpinner className="w-6 h-6 animate-spin text-slate-400" />
              <span className="ml-2 text-slate-300">Loading pipeline data...</span>
            </div>
          ) : !Array.isArray(runs) || runs.length === 0 ? (
            <p className="text-center text-slate-400 py-12">No pipeline runs available</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-slate-700 border-b border-slate-600">
                  <tr>
                    <th className="text-left py-3 px-4 font-semibold text-slate-200">Run ID</th>
                    <th className="text-left py-3 px-4 font-semibold text-slate-200">Status</th>
                    <th className="text-right py-3 px-4 font-semibold text-slate-200">Jobs</th>
                    <th className="text-left py-3 px-4 font-semibold text-slate-200">Completed</th>
                  </tr>
                </thead>
                <tbody>
                  {Array.isArray(runs) && runs.slice(0, 5).map((run) => (
                    <tr key={run.id} className="border-b border-slate-700 hover:bg-slate-700">
                      <td className="py-3 px-4 text-white font-medium">#{run.id}</td>
                      <td className="py-3 px-4">
                        <span
                          className={`inline-block px-3 py-1 rounded-full text-xs font-semibold ${
                            run.status === 'SUCCESS'
                              ? 'bg-green-900 text-green-300'
                              : run.status === 'RUNNING'
                              ? 'bg-blue-900 text-blue-300'
                              : 'bg-red-900 text-red-300'
                          }`}
                        >
                          {run.status}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-right text-white font-medium">
                        {formatNumber(run.jobs_inserted || 0)}
                      </td>
                      <td className="py-3 px-4 text-slate-300">
                        {run.completed_at ? formatDate(run.completed_at) : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

function MetricCard({ title, value, icon, color }) {
  return (
    <div className={`${color} bg-opacity-30 border rounded-lg p-6`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-slate-300">{title}</p>
          <p className="text-2xl font-bold text-white mt-2">{value}</p>
        </div>
        <div>{icon}</div>
      </div>
    </div>
  )
}
