import React, { useState, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useJobs } from '@/api/hooks/useJobs'
import { useFilterStore } from '@/stores/filterStore'
import { useUrlSync } from '@/utils/urlSync'
import { FilterPanel } from '@/components/FilterPanel'
import { formatCurrency, formatNumber, formatDate } from '@/utils/formatters'
import { Loader2, Menu, X, MapPin, DollarSign, Briefcase, Calendar } from 'lucide-react'

export default function JobSearchPage() {
  const navigate = useNavigate()
  const [showMobileFilters, setShowMobileFilters] = useState(false)
  const store = useFilterStore()
  const { searchParams } = useUrlSync()

  // Get all filter params
  const filterParams = useMemo(() => store.getFilterParams(), [store])

  // Fetch jobs
  const { data: jobsData, isLoading, error } = useJobs(filterParams)

  const jobs = jobsData?.jobs || []
  const totalJobs = jobsData?.total || 0
  const totalPages = jobsData?.pages || 1

  // Pagination info
  const paginationInfo = {
    currentPage: store.page,
    pageSize: store.limit,
    totalItems: totalJobs,
    totalPages: totalPages,
    hasNextPage: store.page < totalPages,
    hasPreviousPage: store.page > 1,
  }

  const handleJobClick = (jobId) => {
    navigate(`/jobs/${jobId}`)
  }

  const handlePreviousPage = () => {
    if (paginationInfo.hasPreviousPage) {
      store.setPage(store.page - 1)
    }
  }

  const handleNextPage = () => {
    if (paginationInfo.hasNextPage) {
      store.setPage(store.page + 1)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="flex flex-col md:flex-row h-full">
        {/* Mobile Filter Toggle */}
        <div className="md:hidden sticky top-0 bg-white border-b border-gray-200 px-4 py-3 flex items-center justify-between z-10">
          <h1 className="font-bold text-gray-900">Job Search</h1>
          <button
            onClick={() => setShowMobileFilters(!showMobileFilters)}
            className="p-2 hover:bg-gray-100 rounded-lg transition"
          >
            {showMobileFilters ? (
              <X className="w-6 h-6" />
            ) : (
              <Menu className="w-6 h-6" />
            )}
          </button>
        </div>

        {/* Filters Sidebar */}
        <div
          className={`${
            showMobileFilters ? 'block' : 'hidden'
          } md:block md:w-80 bg-white border-r border-gray-200 overflow-y-auto`}
        >
          <FilterPanel
            isOpen={true}
            onClose={() => setShowMobileFilters(false)}
          />
        </div>

        {/* Main Content */}
        <div className="flex-1 md:min-h-screen px-4 md:px-6 py-8">
          <div className="max-w-4xl mx-auto space-y-6">
            {/* Header */}
            <div>
              <h1 className="text-3xl font-bold text-gray-900 hidden md:block">
                Job Search
              </h1>
              <p className="text-gray-600 mt-2">
                {totalJobs > 0 ? (
                  <>
                    Found <span className="font-semibold">{formatNumber(totalJobs)}</span> job
                    {totalJobs !== 1 ? 's' : ''}
                    {store.hasActiveFilters() && ' matching your filters'}
                  </>
                ) : (
                  'No jobs found'
                )}
              </p>
            </div>

            {/* Search Filters Summary */}
            {store.hasActiveFilters() && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="font-semibold text-blue-900 mb-3">Active Filters</h3>
                    <div className="flex flex-wrap gap-2">
                      {store.keyword && (
                        <FilterTag
                          label={`Keyword: ${store.keyword}`}
                          onRemove={() => store.setKeyword('')}
                        />
                      )}
                      {store.city && (
                        <FilterTag
                          label={`City: ${store.city}`}
                          onRemove={() => store.setCity(null)}
                        />
                      )}
                      {store.country && (
                        <FilterTag
                          label={`Country: ${store.country}`}
                          onRemove={() => store.setCountry(null)}
                        />
                      )}
                      {store.seniority && (
                        <FilterTag
                          label={`Seniority: ${store.seniority}`}
                          onRemove={() => store.setSeniority(null)}
                        />
                      )}
                      {store.minSalary && (
                        <FilterTag
                          label={`Min: ${formatCurrency(store.minSalary)}`}
                          onRemove={() => store.setMinSalary(null)}
                        />
                      )}
                      {store.maxSalary && (
                        <FilterTag
                          label={`Max: ${formatCurrency(store.maxSalary)}`}
                          onRemove={() => store.setMaxSalary(null)}
                        />
                      )}
                      {store.skills.map((skill) => (
                        <FilterTag
                          key={skill}
                          label={skill}
                          onRemove={() => store.removeSkill(skill)}
                        />
                      ))}
                    </div>
                  </div>
                  <button
                    onClick={() => store.reset()}
                    className="text-sm px-3 py-1 text-blue-600 hover:text-blue-700 hover:bg-blue-100 rounded transition whitespace-nowrap"
                  >
                    Clear All
                  </button>
                </div>
              </div>
            )}

            {/* Loading State */}
            {isLoading && (
              <div className="text-center py-12">
                <Loader2 className="w-8 h-8 text-blue-600 animate-spin mx-auto" />
                <p className="mt-4 text-gray-600">Loading jobs...</p>
              </div>
            )}

            {/* Error State */}
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <p className="text-red-700 font-medium">Error loading jobs</p>
                <p className="text-red-600 text-sm mt-1">
                  {error.message || 'Please try again later'}
                </p>
              </div>
            )}

            {/* Job Results */}
            {!isLoading && !error && (
              <>
                <div className="space-y-4">
                  {jobs.length > 0 ? (
                    jobs.map((job, idx) => (
                      <JobCard
                        key={idx}
                        job={job}
                        onClick={() => handleJobClick(job.id || idx)}
                      />
                    ))
                  ) : (
                    <div className="text-center py-12 bg-white rounded-lg border border-gray-200">
                      <Briefcase className="w-12 h-12 text-gray-400 mx-auto mb-3 opacity-50" />
                      <p className="text-gray-600 font-medium">No jobs found</p>
                      <p className="text-gray-500 text-sm mt-1">
                        Try adjusting your filters to expand the search
                      </p>
                    </div>
                  )}
                </div>

                {/* Pagination */}
                {totalPages > 1 && (
                  <div className="flex items-center justify-between bg-white rounded-lg border border-gray-200 p-4">
                    <div className="text-sm text-gray-600">
                      Page{' '}
                      <span className="font-semibold">
                        {paginationInfo.currentPage}
                      </span>{' '}
                      of{' '}
                      <span className="font-semibold">
                        {paginationInfo.totalPages}
                      </span>
                      {' • '}
                      <span className="font-semibold">
                        {formatNumber(paginationInfo.totalItems)}
                      </span>{' '}
                      total jobs
                    </div>
                    <div className="flex gap-2">
                      <button
                        onClick={handlePreviousPage}
                        disabled={!paginationInfo.hasPreviousPage}
                        className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition font-medium text-sm"
                      >
                        Previous
                      </button>
                      <button
                        onClick={handleNextPage}
                        disabled={!paginationInfo.hasNextPage}
                        className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition font-medium text-sm"
                      >
                        Next
                      </button>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

/**
 * JobCard component
 */
function JobCard({ job, onClick }) {
  return (
    <button
      onClick={onClick}
      className="w-full text-left bg-white rounded-lg border border-gray-200 p-6 hover:shadow-md hover:border-blue-300 transition"
    >
      {/* Header */}
      <div className="flex items-start justify-between mb-3">
        <div className="flex-1">
          <h3 className="text-lg font-semibold text-gray-900 hover:text-blue-600 transition">
            {job.title}
          </h3>
          <p className="text-sm text-gray-600 mt-1">{job.company}</p>
        </div>
        {job.salary && (
          <div className="ml-4 flex-shrink-0">
            <p className="text-lg font-bold text-gray-900">
              {formatCurrency(job.salary)}
            </p>
          </div>
        )}
      </div>

      {/* Content */}
      <p className="text-gray-600 text-sm mb-4 line-clamp-2">{job.description}</p>

      {/* Meta Information */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2 mb-4">
        {job.location && (
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <MapPin className="w-4 h-4 flex-shrink-0" />
            {job.location}
          </div>
        )}
        {job.seniority && (
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <Briefcase className="w-4 h-4 flex-shrink-0" />
            {job.seniority}
          </div>
        )}
        {job.posted_date && (
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <Calendar className="w-4 h-4 flex-shrink-0" />
            {formatDate(job.posted_date)}
          </div>
        )}
      </div>

      {/* Skills */}
      {job.required_skills && job.required_skills.length > 0 && (
        <div className="pt-4 border-t border-gray-100">
          <p className="text-xs font-medium text-gray-600 mb-2">Required Skills</p>
          <div className="flex flex-wrap gap-2">
            {job.required_skills.slice(0, 6).map((skill, idx) => (
              <span
                key={idx}
                className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800"
              >
                {skill}
              </span>
            ))}
            {job.required_skills.length > 6 && (
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                +{job.required_skills.length - 6}
              </span>
            )}
          </div>
        </div>
      )}
    </button>
  )
}

/**
 * FilterTag component
 */
function FilterTag({ label, onRemove }) {
  return (
    <div className="inline-flex items-center gap-2 px-3 py-1.5 bg-white border border-blue-300 rounded-full text-sm text-gray-700">
      {label}
      <button
        onClick={onRemove}
        className="text-gray-400 hover:text-gray-600 transition"
      >
        ×
      </button>
    </div>
  )
}
