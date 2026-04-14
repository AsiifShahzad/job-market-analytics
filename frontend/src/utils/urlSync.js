import { useFilterStore } from '@/stores/filterStore'
import { useEffect, useRef } from 'react'
import { useSearchParams } from 'react-router-dom'

/**
 * Hook to sync filter store with URL search params
 * Bidirectional sync: URL params -> store and store -> URL params
 *
 * FIXES:
 * 1. store.getFilterParams() returned a new object every render, making it an
 *    unstable useEffect dependency → infinite loop → blank screen on refresh.
 *    Now we serialize it to a JSON string for stable comparison.
 * 2. The URL→store effect ran on every searchParams change (which the store→URL
 *    effect triggered constantly), causing setFromParams to reset filters mid-fetch.
 *    Now we guard with a ref so only genuine external URL changes sync to the store.
 */
export const useUrlSync = () => {
  const [searchParams, setSearchParams] = useSearchParams()
  const store = useFilterStore()

  // Track whether *we* triggered the last URL update so we don't echo it back
  const isInternalUpdate = useRef(false)

  // --- URL → store (only on genuine external navigation / page load) ---
  useEffect(() => {
    if (isInternalUpdate.current) {
      isInternalUpdate.current = false
      return
    }

    const paramsObj = Object.fromEntries(searchParams)

    // Parse array params (skills)
    if (paramsObj.skills && typeof paramsObj.skills === 'string') {
      paramsObj.skills = paramsObj.skills.split(',').filter(Boolean)
    }

    // Parse number params
    if (paramsObj.page) paramsObj.page = parseInt(paramsObj.page, 10)
    if (paramsObj.limit) paramsObj.limit = parseInt(paramsObj.limit, 10)
    if (paramsObj.min_salary) paramsObj.min_salary = parseFloat(paramsObj.min_salary)
    if (paramsObj.max_salary) paramsObj.max_salary = parseFloat(paramsObj.max_salary)

    store.setFromParams(paramsObj)
  }, [searchParams]) // eslint-disable-line react-hooks/exhaustive-deps
  // Note: intentionally omitting `store` — adding it would re-run after every
  // setFromParams call, creating the same echo loop we're fixing.

  // --- store → URL (stable: serialize params to JSON to avoid new-object churn) ---
  const filterParamsJson = JSON.stringify(store.getFilterParams())

  useEffect(() => {
    const params = JSON.parse(filterParamsJson)
    const newParams = new URLSearchParams()

    Object.entries(params).forEach(([key, value]) => {
      if (
        value === undefined ||
        value === null ||
        value === '' ||
        (Array.isArray(value) && value.length === 0)
      ) {
        return
      }
      if (Array.isArray(value)) {
        newParams.set(key, value.join(','))
      } else {
        newParams.set(key, String(value))
      }
    })

    const currentParams = Object.fromEntries(searchParams)
    const newParamsObj = Object.fromEntries(newParams)

    if (JSON.stringify(currentParams) !== JSON.stringify(newParamsObj)) {
      isInternalUpdate.current = true   // prevent echo back to store
      setSearchParams(newParams, { replace: true })
    }
  }, [filterParamsJson]) // eslint-disable-line react-hooks/exhaustive-deps
  // Note: filterParamsJson is a primitive (string), so this effect only runs
  // when the actual filter values change — not on every render.

  return { searchParams, setSearchParams }
}

/**
 * Helper to get filter params from URL
 */
export const getFilterParamsFromUrl = (searchParams) => {
  const params = Object.fromEntries(searchParams)

  if (params.skills && typeof params.skills === 'string') {
    params.skills = params.skills.split(',').filter(Boolean)
  }

  if (params.page) params.page = parseInt(params.page, 10)
  if (params.limit) params.limit = parseInt(params.limit, 10)
  if (params.min_salary) params.min_salary = parseFloat(params.min_salary)
  if (params.max_salary) params.max_salary = parseFloat(params.max_salary)

  return params
}

/**
 * Helper to create URL with filter params
 */
export const createFilterUrl = (path, filters) => {
  const params = new URLSearchParams()

  Object.entries(filters).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return
    if (Array.isArray(value)) {
      if (value.length > 0) params.set(key, value.join(','))
    } else {
      params.set(key, String(value))
    }
  })

  const queryString = params.toString()
  return queryString ? `${path}?${queryString}` : path
}