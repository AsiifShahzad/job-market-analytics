import React, { useEffect, useState, memo, useMemo } from 'react';
import axios from 'axios';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ScatterChart, Scatter } from 'recharts';

// ── MEMOIZED CHART COMPONENTS ──────────────────────────────────────────

/**
 * Memoized Bar Chart Component
 * Prevents re-creation on every parent render
 */
const MemoizedBarChart = memo(({ data, title, dataKey, fill, height = 450 }) => (
  <div className="bg-slate-800 rounded-lg p-6">
    <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
        <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
        <YAxis tick={{ fill: '#cbd5e1' }} />
        <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }} labelStyle={{ color: '#cbd5e1' }} />
        <Bar dataKey={dataKey} fill={fill} />
      </BarChart>
    </ResponsiveContainer>
  </div>
));

MemoizedBarChart.displayName = 'MemoizedBarChart';

/**
 * Memoized Multi-Bar Chart Component
 * For charts with multiple bars (e.g., current vs previous week)
 */
const MemoizedMultiBarChart = memo(({ data, title, bar1, bar2, fill1, fill2, height = 450 }) => (
  <div className="bg-slate-800 rounded-lg p-6">
    <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
    <ResponsiveContainer width="100%" height={height}>
      <BarChart data={data} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
        <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
        <YAxis tick={{ fill: '#cbd5e1' }} />
        <Tooltip contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }} labelStyle={{ color: '#cbd5e1' }} />
        <Legend wrapperStyle={{ color: '#cbd5e1' }} />
        <Bar dataKey={bar1} fill={fill1} name={bar1} />
        <Bar dataKey={bar2} fill={fill2} name={bar2} />
      </BarChart>
    </ResponsiveContainer>
  </div>
));

MemoizedMultiBarChart.displayName = 'MemoizedMultiBarChart';

/**
 * Memoized Metric Card Component
 */
const MemoizedMetricCard = memo(({ bg, textColor, label, value }) => (
  <div className={`${bg} rounded-lg p-6`}>
    <p className={`${textColor} text-sm`}>{label}</p>
    <p className="text-3xl font-bold text-white">{value}</p>
  </div>
));

MemoizedMetricCard.displayName = 'MemoizedMetricCard';

const AnalyticsDashboard = () => {
  const [analytics, setAnalytics] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('insights');

  useEffect(() => {
    const fetchAnalytics = async () => {
      try {
        const response = await axios.get('/api/analytics/rigorous');
        setAnalytics(response.data);
        setLoading(false);
      } catch (err) {
        setError('Failed to load analytics');
        setLoading(false);
      }
    };
    
    fetchAnalytics();
  }, []);

  // ── MEMOIZED DATA SELECTORS ────────────────────────────────────────────
  // These prevent chart re-creation by memoizing sliced data
  
  const trendingSkillsTop8 = useMemo(
    () => analytics?.trending_skills?.slice(0, 8) || [],
    [analytics?.trending_skills]
  );
  
  const topPayingSkills = useMemo(
    () => analytics?.salary_insights?.top_paying_skills || [],
    [analytics?.salary_insights?.top_paying_skills]
  );
  
  const topCities = useMemo(
    () => analytics?.market_insights?.top_cities || [],
    [analytics?.market_insights?.top_cities]
  );
  
  const topCountries = useMemo(
    () => analytics?.market_insights?.top_countries || [],
    [analytics?.market_insights?.top_countries]
  );
  
  const skillInsights = useMemo(
    () => analytics?.skill_insights || {},
    [analytics?.skill_insights]
  );
  
  const trendingSkills = useMemo(
    () => analytics?.trending_skills || [],
    [analytics?.trending_skills]
  );
  
  const salaryInsights = useMemo(
    () => analytics?.salary_insights || {},
    [analytics?.salary_insights]
  );
  
  const marketInsights = useMemo(
    () => analytics?.market_insights || {},
    [analytics?.market_insights]
  );
  
  const actionableInsights = useMemo(
    () => analytics?.actionable_insights || [],
    [analytics?.actionable_insights]
  );
  
  const dataQualityReport = useMemo(
    () => analytics?.data_quality_report || {},
    [analytics?.data_quality_report]
  );

  if (loading) return <div className="p-8 text-center">Loading analytics...</div>;
  if (error) return <div className="p-8 text-center text-red-600">{error}</div>;
  if (!analytics) return <div className="p-8 text-center">No data available</div>;

  return (
    <div className="min-h-screen bg-slate-900">
      <div className="container mx-auto p-6">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">Job Market Analytics</h1>
          <p className="text-slate-300">Statistically rigorous, bias-free market insights powered by AI/NLP</p>
        </div>

        {/* Tabs */}
        <div className="flex gap-2 mb-8 flex-wrap">
          {['insights', 'skills', 'trending', 'salary', 'market', 'quality'].map(tab => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 rounded font-semibold transition ${
                activeTab === tab
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-800 text-slate-300 hover:bg-slate-700'
              }`}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
        </div>

        {/* ACTIONABLE INSIGHTS */}
        {activeTab === 'insights' && (
          <div className="space-y-4">
            <h2 className="text-2xl font-bold text-white mb-6">Executive Summary</h2>
            {analytics.actionable_insights && analytics.actionable_insights.length > 0 ? (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {analytics.actionable_insights.map((insight, idx) => (
                  <div key={idx} className="bg-slate-800 border border-slate-700 rounded-lg p-6">
                    <div className="flex items-start justify-between mb-3">
                      <div className="flex-1">
                        <p className="text-white text-lg">{insight.text}</p>
                      </div>
                      <span className={`ml-4 px-3 py-1 rounded text-sm font-bold whitespace-nowrap ${
                        insight.confidence === 'HIGH' ? 'bg-green-900 text-green-300' :
                        insight.confidence === 'MEDIUM' ? 'bg-yellow-900 text-yellow-300' :
                        'bg-red-900 text-red-300'
                      }`}>
                        {insight.confidence}
                      </span>
                    </div>
                    <p className="text-slate-400 text-sm mb-2">{insight.reason}</p>
                    <p className="text-slate-500 text-xs">Sample size: {insight.sample_size.toLocaleString()}</p>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-slate-400">No high-confidence insights available</p>
            )}
          </div>
        )}

        {/* SKILL DEMAND */}
        {activeTab === 'skills' && (
          <div className="space-y-8">
            <h2 className="text-2xl font-bold text-white mb-6">Skill Demand by Category</h2>
            {Object.entries(skillInsights).map(([category, skills]) => (
              <div key={category}>
                <h3 className="text-xl font-semibold text-blue-400 mb-4 capitalize">{category}</h3>
                <div className="bg-slate-800 rounded-lg p-6">
                  <ResponsiveContainer width="100%" height={450}>
                    <BarChart data={skills.slice(0, 10)} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                      <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                      <YAxis tick={{ fill: '#cbd5e1' }} />
                      <Tooltip 
                        contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                        labelStyle={{ color: '#cbd5e1' }}
                      />
                      <Bar dataKey="percentage" fill="#3b82f6" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            ))}
          </div>
        )}

        {/* TRENDING SKILLS */}
        {activeTab === 'trending' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white mb-6">Trending Skills (Week-over-Week Growth)</h2>
            {trendingSkills && trendingSkills.length > 0 ? (
              <>
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  {/* Growth Chart */}
                  <div className="bg-slate-800 rounded-lg p-6">
                    <h3 className="text-lg font-semibold text-white mb-4">Growth Rates</h3>
                    <ResponsiveContainer width="100%" height={450}>
                      <BarChart data={analytics.trending_skills.slice(0, 8)} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                        <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                        <YAxis tick={{ fill: '#cbd5e1' }} />
                        <Tooltip 
                          contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                          labelStyle={{ color: '#cbd5e1' }}
                        />
                        <Bar dataKey="growth_rate" fill="#10b981" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>

                  {/* Job Count Trend */}
                  <div className="bg-slate-800 rounded-lg p-6">
                    <h3 className="text-lg font-semibold text-white mb-4">Job Count Comparison</h3>
                    <ResponsiveContainer width="100%" height={450}>
                      <BarChart data={trendingSkillsTop8} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                        <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                        <YAxis tick={{ fill: '#cbd5e1' }} />
                        <Tooltip 
                          contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                          labelStyle={{ color: '#cbd5e1' }}
                        />
                        <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                        <Bar dataKey="current_week" fill="#3b82f6" name="Current Week" />
                        <Bar dataKey="previous_week" fill="#8b5cf6" name="Previous Week" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                {/* Trending List */}
                <div className="bg-slate-800 rounded-lg p-6">
                  <h3 className="text-lg font-semibold text-white mb-4">Trending Skills Ranked</h3>
                  <div className="space-y-3">
                    {trendingSkills.map((skill, idx) => (
                      <div key={idx} className="flex items-center justify-between p-3 bg-slate-700 rounded">
                        <div className="flex-1">
                          <p className="text-white font-medium">{skill.skill}</p>
                          <p className="text-slate-400 text-sm">
                            {skill.current_week} jobs (prev: {skill.previous_week})
                          </p>
                        </div>
                        <div className="text-right">
                          <p className={`text-lg font-bold ${skill.growth_rate > 0 ? 'text-green-400' : 'text-red-400'}`}>
                            {skill.growth_rate > 0 ? '+' : ''}{skill.growth_rate}%
                          </p>
                          <p className="text-slate-400 text-xs">{skill.confidence}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            ) : (
              <p className="text-slate-400">No trending skills data available</p>
            )}
          </div>
        )}

        {/* SALARY INSIGHTS */}
        {activeTab === 'salary' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white mb-6">Salary Analysis</h2>
            
            {/* Top Paying Skills */}
            {topPayingSkills && topPayingSkills.length > 0 && (
              <div className="bg-slate-800 rounded-lg p-6">
                <h3 className="text-lg font-semibold text-white mb-4">💰 Top Paying Skills</h3>
                <ResponsiveContainer width="100%" height={450}>
                  <BarChart data={topPayingSkills} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                    <XAxis dataKey="skill" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                    <YAxis tick={{ fill: '#cbd5e1' }} />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                      labelStyle={{ color: '#cbd5e1' }}
                      formatter={(value) => `$${value.toLocaleString()}`}
                    />
                    <Legend wrapperStyle={{ color: '#cbd5e1' }} />
                    <Bar dataKey="median_salary" fill="#f59e0b" name="Median" />
                    <Bar dataKey="avg_salary" fill="#3b82f6" name="Average" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Salary by Seniority */}
            {salaryInsights?.by_seniority && (
              <div className="bg-slate-800 rounded-lg p-6">
                <h3 className="text-lg font-semibold text-white mb-4">📊 Salary by Seniority Level</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
                  {Object.entries(salaryInsights.by_seniority).map(([level, data]) => (
                    <div key={level} className="bg-slate-700 p-4 rounded">
                      <p className="text-slate-400 text-sm capitalize">{level}</p>
                      <p className="text-white text-2xl font-bold">${(data.median_salary / 1000).toFixed(0)}k</p>
                      <p className="text-slate-500 text-xs">{data.sample_size} samples</p>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* MARKET INSIGHTS */}
        {activeTab === 'market' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white mb-6">Market Insights</h2>
            
            {/* Key Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-blue-900 rounded-lg p-6">
                <p className="text-blue-300 text-sm">Total Jobs Analyzed</p>
                <p className="text-3xl font-bold text-white">{(marketInsights?.total_jobs || 0).toLocaleString()}</p>
              </div>
              <div className="bg-green-900 rounded-lg p-6">
                <p className="text-green-300 text-sm">Remote Jobs</p>
                <p className="text-3xl font-bold text-white">{(marketInsights?.remote_jobs || 0).toLocaleString()}</p>
              </div>
              <div className="bg-purple-900 rounded-lg p-6">
                <p className="text-purple-300 text-sm">Remote Percentage</p>
                <p className="text-3xl font-bold text-white">{(marketInsights?.remote_percentage || 0).toFixed(1)}%</p>
              </div>
            </div>

            {/* Top Cities */}
            {topCities && topCities.length > 0 && (
              <div className="bg-slate-800 rounded-lg p-6">
                <h3 className="text-lg font-semibold text-white mb-4">🏙️ Top Cities</h3>
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
            )}

            {/* Top Countries */}
            {topCountries && topCountries.length > 0 && (
              <div className="bg-slate-800 rounded-lg p-6">
                <h3 className="text-lg font-semibold text-white mb-4">🌍 Top Countries</h3>
                <ResponsiveContainer width="100%" height={450}>
                  <BarChart data={topCountries} margin={{ top: 20, right: 30, left: 0, bottom: 100 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
                    <XAxis dataKey="country" tick={{ fill: '#cbd5e1', fontSize: 12 }} angle={-45} textAnchor="end" height={80} />
                    <YAxis tick={{ fill: '#cbd5e1' }} />
                    <Tooltip 
                      contentStyle={{ backgroundColor: '#1e293b', border: 'none', borderRadius: '8px' }}
                      labelStyle={{ color: '#cbd5e1' }}
                    />
                    <Bar dataKey="job_count" fill="#10b981" />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
        )}

        {/* DATA QUALITY REPORT */}
        {activeTab === 'quality' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white mb-6">📋 Data Quality Report</h2>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="bg-slate-800 rounded-lg p-6">
                <p className="text-slate-400 text-sm mb-2">Jobs Before Cleaning</p>
                <p className="text-3xl font-bold text-white">{(dataQualityReport?.jobs_before_cleaning || 0).toLocaleString()}</p>
              </div>
              <div className="bg-slate-800 rounded-lg p-6">
                <p className="text-slate-400 text-sm mb-2">Jobs After Cleaning</p>
                <p className="text-3xl font-bold text-white">{(analytics.data_quality_report?.jobs_after_cleaning || 0).toLocaleString()}</p>
              </div>
              <div className="bg-slate-800 rounded-lg p-6">
                <p className="text-slate-400 text-sm mb-2">Data Retention Rate</p>
                <p className="text-3xl font-bold text-white">
                  {analytics.data_quality_report && analytics.data_quality_report.jobs_before_cleaning > 0
                    ? ((analytics.data_quality_report.jobs_after_cleaning / analytics.data_quality_report.jobs_before_cleaning) * 100).toFixed(1)
                    : 0
                  }%
                </p>
              </div>
              <div className="bg-slate-800 rounded-lg p-6">
                <p className="text-slate-400 text-sm mb-2">Skills Validated</p>
                <p className="text-3xl font-bold text-white">{(analytics.data_quality_report?.skills_validated || 0).toLocaleString()}</p>
              </div>
            </div>

            <div className="bg-slate-800 rounded-lg p-6">
              <h3 className="text-lg font-semibold text-white mb-4">Cleaning Actions</h3>
              <ul className="space-y-2 text-slate-300">
                <li>• Invalid skills removed: {(analytics.data_quality_report?.skills_removed_as_noise || 0)}</li>
                <li>• Invalid locations removed: {(analytics.data_quality_report?.invalid_locations_removed?.length || 0)}</li>
                <li>• Jobs with missing critical fields: {(analytics.data_quality_report?.jobs_before_cleaning || 0) - (analytics.data_quality_report?.jobs_after_cleaning || 0)}</li>
              </ul>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default AnalyticsDashboard;
