import React, { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  BarChart,
  Bar,
  LineChart,
  Line,
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
import { formatCurrency, formatNumber, formatDate, formatTfIdfScore } from '@/utils/formatters.js'
import { Loader2, TrendingUp, Activity, ArrowRight, Search, Briefcase } from 'lucide-react'

// Mock data for development/when API is unavailable
const MOCK_SKILLS = [
  { name: 'Python', frequency: 5000, average_salary: 120000 },
  { name: 'JavaScript', frequency: 4800, average_salary: 115000 },
  { name: 'React', frequency: 4200, average_salary: 125000 },
  { name: 'Java', frequency: 3900, average_salary: 130000 },
  { name: 'SQL', frequency: 3700, average_salary: 110000 },
  { name: 'AWS', frequency: 3500, average_salary: 140000 },
  { name: 'Node.js', frequency: 3300, average_salary: 118000 },
  { name: 'Docker', frequency: 3100, average_salary: 135000 },
  { name: 'Kubernetes', frequency: 2900, average_salary: 145000 },
  { name: 'TypeScript', frequency: 2700, average_salary: 122000 },
]

const MOCK_EMERGING = [
  { name: 'AI/ML', growth_rate: 0.45, job_count: 1200 },
  { name: 'Prompt Engineering', growth_rate: 0.38, job_count: 850 },
  { name: 'Web3', growth_rate: 0.32, job_count: 650 },
  { name: 'Cloud Native', growth_rate: 0.28, job_count: 920 },
  { name: 'DevOps', growth_rate: 0.25, job_count: 780 },
  { name: 'GraphQL', growth_rate: 0.22, job_count: 540 },
]

const MOCK_RUNS = [
  { id: 1, status: 'SUCCESS', jobs_inserted: 5234, completed_at: new Date().toISOString(), unique_skills: 432 },
  { id: 2, status: 'SUCCESS', jobs_inserted: 4891, completed_at: new Date(Date.now() - 86400000).toISOString(), unique_skills: 418 },
]

export default function Dashboard() {
  const navigate = useNavigate()
  const { data: skillsData, isLoading: skillsLoading, error: skillsError } = useSkills()
  const { data: emergingData, isLoading: emergingLoading, error: emergingError } = useEmergingSkills()
  const { data: runsData, isLoading: runsLoading, error: runsError } = usePipelineRuns()

  // Use mock data when actual data is loading or unavailable
  // Handle both array and object responses from API
  const skillsArray = Array.isArray(skillsData?.skills)
    ? skillsData.skills
    : Array.isArray(skillsData?.data?.skills)
    ? skillsData.data.skills
    : MOCK_SKILLS

  const emergeArray = Array.isArray(emergingData?.emerging_skills)
    ? emergingData.emerging_skills
    : Array.isArray(emergingData?.data?.trending_skills)
    ? emergingData.data.trending_skills
    : MOCK_EMERGING

  const runsArray = Array.isArray(runsData?.runs)
    ? runsData.runs
    : Array.isArray(runsData?.data?.runs)
    ? runsData.data.runs
    : MOCK_RUNS

  const skills = Array.isArray(skillsArray) ? skillsArray : MOCK_SKILLS
  const emerging = Array.isArray(emergeArray) ? emergeArray : MOCK_EMERGING
  const runs = Array.isArray(runsArray) ? runsArray : MOCK_RUNS

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
    <div className="min-h-screen bg-gray-50 px-4 md:px-6 py-8">
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-4xl font-bold text-gray-900">Dashboard</h1>
            <p className="text-gray-600 mt-2">Monitor job market trends and skill demand</p>
          </div>
          <button
            onClick={handleGoToJobs}
            className="hidden md:inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition font-medium"
          >
            <Search className="w-4 h-4" />
            Search Jobs
          </button>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <MetricCard
            title="Total Jobs"
            value={formatNumber(totalJobs)}
            icon={<Activity className="w-6 h-6 text-blue-600" />}
            color="bg-blue-50"
          />
          <MetricCard
            title="Unique Skills"
            value={formatNumber(uniqueSkills)}
            icon={<TrendingUp className="w-6 h-6 text-purple-600" />}
            color="bg-purple-50"
          />
          <MetricCard
            title="Last Update"
            value={latestRun?.completed_at ? formatDate(latestRun.completed_at) : 'Recently'}
            icon={<Activity className="w-6 h-6 text-green-600" />}
            color="bg-green-50"
          />
        </div>

        {/* CTA Banner */}
        <div className="bg-gradient-to-r from-blue-600 to-blue-700 rounded-lg shadow-lg p-6 md:p-8 text-white">
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
              <ArrowRight className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Top Skills Chart */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-6">Top 10 In-Demand Skills</h2>
          {skillsLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
              <span className="ml-2 text-gray-600">Loading skills...</span>
            </div>
          ) : chartData.length === 0 ? (
            <p className="text-center text-gray-500 py-12">No skills data available</p>
          ) : (
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="name" stroke="#6b7280" angle={-45} textAnchor="end" height={100} />
                <YAxis stroke="#6b7280" />
                <Tooltip
                  contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '8px' }}
                  formatter={(value) => formatNumber(value)}
                />
                <Legend />
                <Bar dataKey="frequency" fill="#a855f7" name="Job Count" />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Emerging Skills */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-6">Emerging Skills (Recent Trend)</h2>
          {emergingLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
              <span className="ml-2 text-gray-600">Loading trends...</span>
            </div>
          ) : !Array.isArray(emerging) || emerging.length === 0 ? (
            <p className="text-center text-gray-500 py-12">No emerging skills data available</p>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {Array.isArray(emerging) && emerging.slice(0, 6).map((skill) => (
                <div key={skill.name} className="p-4 border border-gray-200 rounded-lg hover:border-purple-300 transition">
                  <div className="flex items-center justify-between mb-2">
                    <button
                      onClick={() => handleSkillClick(skill.name)}
                      className="font-semibold text-gray-900 hover:text-purple-600 transition"
                    >
                      {skill.name}
                    </button>
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  </div>
                  <p className="text-sm text-gray-600 mb-3">
                    Growth: <span className="font-semibold text-green-600">{formatTfIdfScore(skill.growth_rate || 0)}</span>
                  </p>
                  <p className="text-xs text-gray-500 mb-3">
                    Jobs: {formatNumber(skill.job_count || 0)}
                  </p>
                  <button
                    onClick={() => handleSearchBySkill(skill.name)}
                    className="w-full text-xs px-3 py-1.5 border border-purple-200 text-purple-600 rounded hover:bg-purple-50 transition font-medium"
                  >
                    View Jobs
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Pipeline Runs */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-6">Recent Pipeline Runs</h2>
          {runsLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
              <span className="ml-2 text-gray-600">Loading pipeline data...</span>
            </div>
          ) : !Array.isArray(runs) || runs.length === 0 ? (
            <p className="text-center text-gray-500 py-12">No pipeline runs available</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="text-left py-3 px-4 font-semibold text-gray-700">Run ID</th>
                    <th className="text-left py-3 px-4 font-semibold text-gray-700">Status</th>
                    <th className="text-right py-3 px-4 font-semibold text-gray-700">Jobs</th>
                    <th className="text-left py-3 px-4 font-semibold text-gray-700">Completed</th>
                  </tr>
                </thead>
                <tbody>
                  {Array.isArray(runs) && runs.slice(0, 5).map((run) => (
                    <tr key={run.id} className="border-b border-gray-200 hover:bg-gray-50">
                      <td className="py-3 px-4 text-gray-900 font-medium">#{run.id}</td>
                      <td className="py-3 px-4">
                        <span
                          className={`inline-block px-3 py-1 rounded-full text-xs font-semibold ${
                            run.status === 'SUCCESS'
                              ? 'bg-green-50 text-green-700'
                              : run.status === 'RUNNING'
                              ? 'bg-blue-50 text-blue-700'
                              : 'bg-red-50 text-red-700'
                          }`}
                        >
                          {run.status}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-right text-gray-900 font-medium">
                        {formatNumber(run.jobs_inserted || 0)}
                      </td>
                      <td className="py-3 px-4 text-gray-600">
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
    <div className={`${color} border border-gray-200 rounded-lg p-6`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-900 mt-2">{value}</p>
        </div>
        <div>{icon}</div>
      </div>
    </div>
  )
}
