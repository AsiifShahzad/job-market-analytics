/**
 * Comprehensive Insights Dashboard
 * Displays high-impact, actionable insights for the job market.
 */

import React, { useEffect, useState, useMemo } from 'react'
import { FaSpinner, FaArrowUp, FaDollarSign, FaMapPin, FaBolt, FaChartColumn } from 'react-icons/fa6'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import axios from 'axios'
import { useInsightsSummary, useSkillsInsights, useSalaryInsights, useMarketInsights } from '@/api/hooks/useInsights'
import { formatCurrency, formatNumber } from '@/utils/formatters'

export default function InsightsDashboard() {
  // Fetch all insights
  const { data: summary, isLoading: summaryLoading } = useInsightsSummary()
  const { data: skillsData, isLoading: skillsLoading } = useSkillsInsights()
  const { data: salaryData, isLoading: salaryLoading } = useSalaryInsights()
  const { data: marketData, isLoading: marketLoading } = useMarketInsights()
  
  // Fetch analytics for top cities graph
  const [analytics, setAnalytics] = useState(null)
  const [analyticsLoading, setAnalyticsLoading] = useState(true)

  useEffect(() => {
    const fetchAnalytics = async () => {
      try {
        const response = await axios.get('/api/analytics/rigorous')
        setAnalytics(response.data)
        setAnalyticsLoading(false)
      } catch (err) {
        console.error('Failed to load analytics:', err)
        setAnalyticsLoading(false)
      }
    }
    fetchAnalytics()
  }, [])

  const isLoading = summaryLoading || skillsLoading || salaryLoading || marketLoading || analyticsLoading
  
  // Calculate loading progress
  const loadingSteps = [
    { label: 'Loading insights...', active: summaryLoading },
    { label: 'Analyzing skills...', active: skillsLoading },
    { label: 'Processing salaries...', active: salaryLoading },
    { label: 'Gathering market data...', active: marketLoading },
    { label: 'Preparing visualizations...', active: analyticsLoading }
  ]
  
  const completedSteps = loadingSteps.filter(s => !s.active).length
  
  // ── MEMOIZED DATA SELECTORS ────────────────────────────────────────────
  // Prevent chart re-creation by memoizing data
  
  const topCities = useMemo(
    () => analytics?.market_insights?.top_cities || [],
    [analytics?.market_insights?.top_cities]
  )

  return (
    <div className="min-h-screen bg-slate-900 px-4 md:px-6 py-8">
      <div className="max-w-7xl mx-auto space-y-8">
        {/* Header */}
        <div>
          <h1 className="text-4xl font-bold text-white">Job Market Insights</h1>
          <p className="text-slate-300 mt-2">Data-driven insights to guide your career decisions</p>
        </div>

        {isLoading ? (
          <div className="min-h-[60vh] flex items-center justify-center">
            <div className="w-full max-w-md space-y-6">
              {/* Main Loading Spinner */}
              <div className="text-center">
                <FaSpinner className="w-12 h-12 text-blue-400 animate-spin mx-auto mb-4" />
                <p className="text-xl font-semibold text-white mb-2">Preparing your insights...</p>
                <p className="text-slate-400 text-sm">One moment while we gather and analyze your data</p>
              </div>
              
              {/* Progress Steps */}
              <div className="space-y-2 bg-slate-800 rounded-lg p-4 border border-slate-700">
                {loadingSteps.map((step, idx) => (
                  <div key={idx} className="flex items-center gap-3">
                    <div className={`flex-shrink-0 w-5 h-5 rounded-full flex items-center justify-center text-xs font-bold ${ step.active ? 'bg-blue-500 text-white animate-pulse' : 'bg-green-600 text-white'}`}>
                      {step.active ? '...' : '✓'}
                    </div>
                    <span className={`text-sm ${ step.active ? 'text-white font-medium' : 'text-slate-400'}`}>
                      {step.label}
                    </span>
                  </div>
                ))}
              </div>
              
              {/* Progress Bar */}
              <div className="w-full bg-slate-700 rounded-full h-2 overflow-hidden">
                <div 
                  className="h-full bg-gradient-to-r from-blue-500 to-blue-400 transition-all duration-500"
                  style={{ width: `${(completedSteps / loadingSteps.length) * 100}%` }}
                />
              </div>
              
              <p className="text-center text-xs text-slate-400">
                {completedSteps} of {loadingSteps.length} steps completed
              </p>
            </div>
          </div>
        ) : (
          <>
            {/* Key Insights Section */}
            <section className="bg-gradient-to-r from-blue-600 to-blue-700 text-white rounded-xl shadow-lg p-8">
              <div className="flex items-center gap-3 mb-6">
                <FaBolt className="w-6 h-6" />
                <h2 className="text-2xl font-bold">🔥 Key Insights</h2>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {summary?.actionable_insights?.length > 0 ? (
                  summary.actionable_insights.slice(0, 4).map((insight, idx) => (
                    <div key={idx} className="bg-white bg-opacity-10 backdrop-blur rounded-lg p-4">
                      <p className="text-sm leading-relaxed">{insight}</p>
                    </div>
                  ))
                ) : (
                  <p className="text-white text-opacity-80">No insights available yet. Run the pipeline to collect more data.</p>
                )}
              </div>
            </section>

            {/* Skill Demand Section */}
            <section className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
              <div className="flex items-center gap-3 mb-6">
                <FaChartColumn className="w-6 h-6 text-blue-400" />
                <h2 className="text-2xl font-bold text-white">📊 Skill Demand</h2>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Top Skills */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Top Skills by Demand</h3>
                  <div className="space-y-3">
                    {skillsData?.top_skills?.length > 0 ? (
                      skillsData.top_skills.map((skill, idx) => (
                        <div key={idx} className="flex items-center justify-between bg-slate-700 rounded p-3">
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <span className="font-semibold text-white">{skill.skill}</span>
                              {skill.label && <span className="text-xs bg-yellow-900 text-yellow-300 px-2 py-1 rounded">{skill.label}</span>}
                            </div>
                            <p className="text-xs text-slate-400 mt-1">{skill.category}</p>
                          </div>
                          <div className="text-right">
                            <p className="font-bold text-white">{formatNumber(skill.demand)}</p>
                            <p className="text-xs text-slate-400">jobs</p>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-slate-400">No skill data available</p>
                    )}
                  </div>
                </div>

                {/* Trending Skills */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">📈 Trending Skills</h3>
                  <div className="space-y-3">
                    {skillsData?.trending_skills?.length > 0 ? (
                      skillsData.trending_skills.map((skill, idx) => (
                        <div key={idx} className="flex items-center justify-between bg-green-900 bg-opacity-30 border border-green-700 rounded p-3">
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <FaArrowUp className="w-4 h-4 text-green-400" />
                              <span className="font-semibold text-white">{skill.skill}</span>
                              {skill.label && <span className="text-xs bg-green-900 text-green-300 px-2 py-1 rounded">{skill.label}</span>}
                            </div>
                          </div>
                          <div className="text-right">
                            <p className="font-bold text-green-400">{skill.growth_rate > 0 ? '+' : ''}{skill.growth_rate}%</p>
                            <p className="text-xs text-slate-400">WoW growth</p>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-slate-400">No trending data available</p>
                    )}
                  </div>
                </div>
              </div>
            </section>

            {/* Salary Insights Section */}
            <section className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
              <div className="flex items-center gap-3 mb-6">
                <FaDollarSign className="w-6 h-6 text-green-400" />
                <h2 className="text-2xl font-bold text-white">💰 Salary Insights</h2>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                {/* Top Paying Skills */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Highest Paying Skills</h3>
                  <div className="space-y-3">
                    {salaryData?.top_paying_skills?.length > 0 ? (
                      salaryData.top_paying_skills.map((skill, idx) => (
                        <div key={idx} className="bg-gradient-to-r from-green-900 to-blue-900 bg-opacity-30 rounded p-3">
                          <div className="flex items-center justify-between mb-1">
                            <span className="font-semibold text-white">{skill.skill}</span>
                            {skill.label && <span className="text-xs bg-green-900 text-green-300 px-2 py-1 rounded">{skill.label}</span>}
                          </div>
                          <p className="text-lg font-bold text-green-400">{formatCurrency(skill.avg_salary)}</p>
                          <p className="text-xs text-slate-400">avg salary</p>
                        </div>
                      ))
                    ) : (
                      <p className="text-slate-400">No salary data available</p>
                    )}
                  </div>
                </div>

                {/* Salary by Seniority */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Salary by Experience</h3>
                  <div className="space-y-2">
                    {salaryData?.by_seniority && Object.entries(salaryData.by_seniority).map(([level, data]) => (
                      <div key={level} className="bg-slate-700 rounded p-3">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="font-semibold text-white capitalize">{level}</p>
                            <p className="text-xs text-slate-400">{data.job_count} jobs</p>
                          </div>
                          <p className="font-bold text-white">{formatCurrency(data.avg_salary)}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Remote Comparison */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Remote vs Office</h3>
                  {salaryData?.remote_comparison?.insight && (
                    <div className="bg-blue-900 border border-blue-700 rounded p-4 mb-4">
                      <p className="text-sm text-blue-300 font-medium">{salaryData.remote_comparison.insight}</p>
                    </div>
                  )}
                  <div className="space-y-2">
                    <div className="bg-slate-700 rounded p-3">
                      <p className="text-sm text-slate-400">Remote</p>
                      <p className="text-lg font-bold text-white">{formatCurrency(salaryData?.remote_comparison?.remote?.avg_salary || 0)}</p>
                    </div>
                    <div className="bg-slate-700 rounded p-3">
                      <p className="text-sm text-slate-400">Non-remote</p>
                      <p className="text-lg font-bold text-white">{formatCurrency(salaryData?.remote_comparison?.non_remote?.avg_salary || 0)}</p>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            {/* Market Overview Section */}
            <section className="bg-slate-800 border border-slate-700 rounded-lg shadow p-6">
              <div className="flex items-center gap-3 mb-6">
                <FaMapPin className="w-6 h-6 text-red-400" />
                <h2 className="text-2xl font-bold text-white">🌍 Market Overview</h2>
              </div>

              {/* Top Cities Graph */}
              {analytics?.market_insights?.top_cities && (
                <div className="mb-8">
                  <h3 className="text-lg font-semibold text-white mb-4">🏙️ Top Cities</h3>
                  <div className="bg-slate-700 bg-opacity-50 rounded-lg p-4">
                    <ResponsiveContainer width="100%" height={450}>
                      <BarChart data={topCities} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                        <XAxis dataKey="city" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                        <YAxis tick={{ fill: '#cbd5e1' }} />
                        <Tooltip 
                          contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                          labelStyle={{ color: '#cbd5e1' }}
                        />
                        <Bar dataKey="job_count" fill="#3b82f6" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}

              <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Top Locations */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Top Hiring Cities</h3>
                  <div className="space-y-2">
                    {marketData?.top_locations?.length > 0 ? (
                      marketData.top_locations.map((loc, idx) => (
                        <div key={idx} className="flex items-center justify-between bg-slate-700 rounded p-3">
                          <div>
                            <p className="font-semibold text-white">{loc.city}</p>
                            <p className="text-xs text-slate-400">{loc.country}</p>
                          </div>
                          <p className="font-bold text-white">{formatNumber(loc.job_count)}</p>
                        </div>
                      ))
                    ) : (
                      <p className="text-slate-400">No location data available</p>
                    )}
                  </div>
                </div>

                {/* Remote Stats */}
                <div>
                  <h3 className="text-lg font-semibold text-slate-200 mb-4">Remote Job Market</h3>
                  {marketData?.remote_percentage && (
                    <div className="bg-gradient-to-br from-purple-900 to-blue-900 bg-opacity-30 rounded p-6 mb-4">
                      <div className="text-center">
                        <p className="text-5xl font-bold text-purple-400">{marketData.remote_percentage.percentage}%</p>
                        <p className="text-slate-300 mt-2">of jobs are remote</p>
                      </div>
                      <div className="mt-4 pt-4 border-t border-purple-700 text-sm text-slate-300">
                        <p>{formatNumber(marketData.remote_percentage.remote_jobs)} remote jobs</p>
                        <p>{formatNumber(marketData.remote_percentage.total_jobs)} total jobs</p>
                      </div>
                    </div>
                  )}
                </div>
              </div>

              {/* Seniority Distribution */}
              <div className="mt-8 pt-8 border-t border-slate-700">
                <h3 className="text-lg font-semibold text-slate-200 mb-4">Job Distribution by Experience Level</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  {marketData?.seniority_distribution && Object.entries(marketData.seniority_distribution).map(([level, data]) => (
                    <div key={level} className="bg-blue-900 bg-opacity-30 rounded p-4 text-center">
                      <p className="text-3xl font-bold text-blue-400">{data.percentage}%</p>
                      <p className="text-sm text-slate-200 mt-1 capitalize">{level}</p>
                      <p className="text-xs text-slate-400">{data.count} jobs</p>
                    </div>
                  ))}
                </div>
              </div>
            </section>

            {/* Data Quality Notice */}
            <div className="bg-yellow-900 border border-yellow-700 rounded-lg p-4 text-sm text-yellow-300">
              <p>
                💡 <strong>Tips for best insights:</strong> Run the pipeline multiple times to collect more data. Insights are based on verified job listings from the last 30 days.
              </p>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
