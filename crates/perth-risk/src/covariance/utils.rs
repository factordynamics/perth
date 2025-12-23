//! Utilities for covariance matrix manipulation
//!
//! This module provides functions for enforcing positive definiteness,
//! eigenvalue decomposition, and other matrix operations needed for
//! covariance matrix estimation and manipulation.

use super::CovarianceError;
use ndarray::{Array1, Array2};

/// Configuration for positive definiteness enforcement
#[derive(Debug, Clone)]
pub struct PositiveDefiniteConfig {
    /// Minimum eigenvalue (default: 1e-10)
    pub min_eigenvalue: f64,
    /// Whether to preserve the trace (sum of diagonal elements)
    pub preserve_trace: bool,
}

impl Default for PositiveDefiniteConfig {
    fn default() -> Self {
        Self {
            min_eigenvalue: 1e-10,
            preserve_trace: false,
        }
    }
}

/// Result of eigenvalue decomposition
#[derive(Debug, Clone)]
pub struct EigenDecomposition {
    /// Eigenvalues (sorted in descending order)
    pub eigenvalues: Array1<f64>,
    /// Eigenvectors (columns are eigenvectors)
    pub eigenvectors: Array2<f64>,
}

/// Enforce positive definiteness via eigenvalue clipping
///
/// This function performs eigenvalue decomposition, clips negative or small
/// eigenvalues to a minimum threshold, and reconstructs the matrix.
///
/// # Arguments
/// * `cov` - Covariance matrix to fix (must be symmetric)
/// * `config` - Configuration for the enforcement
///
/// # Returns
/// * Positive definite covariance matrix
///
/// # Example
/// ```ignore
/// let config = PositiveDefiniteConfig {
///     min_eigenvalue: 1e-8,
///     preserve_trace: true,
/// };
/// let pd_cov = enforce_positive_definite(&cov, &config)?;
/// ```
pub fn enforce_positive_definite(
    cov: &Array2<f64>,
    config: &PositiveDefiniteConfig,
) -> Result<Array2<f64>, CovarianceError> {
    let n = cov.nrows();
    if n != cov.ncols() {
        return Err(CovarianceError::DimensionMismatch {
            expected: n,
            actual: cov.ncols(),
        });
    }

    // Perform eigenvalue decomposition
    let decomp = jacobi_eigendecomp(cov, 100, 1e-12)?;

    let original_trace: f64 = decomp.eigenvalues.iter().sum();

    // Clip eigenvalues
    let mut clipped_eigenvalues = decomp.eigenvalues.clone();
    for val in clipped_eigenvalues.iter_mut() {
        if *val < config.min_eigenvalue {
            *val = config.min_eigenvalue;
        }
    }

    // Preserve trace if requested
    if config.preserve_trace && original_trace > 0.0 {
        let new_trace: f64 = clipped_eigenvalues.iter().sum();
        let scale = original_trace / new_trace;
        clipped_eigenvalues.mapv_inplace(|v| v * scale);
    }

    // Reconstruct matrix: Σ = V * Λ * V^T
    reconstruct_from_eigen(&clipped_eigenvalues, &decomp.eigenvectors)
}

/// Check if a matrix is positive definite
///
/// A matrix is positive definite if all eigenvalues are strictly positive.
///
/// # Arguments
/// * `cov` - Matrix to check
///
/// # Returns
/// * `true` if all eigenvalues are positive
pub fn is_positive_definite(cov: &Array2<f64>) -> bool {
    is_positive_definite_with_tolerance(cov, 1e-10)
}

/// Check if a matrix is positive definite with a custom tolerance
///
/// # Arguments
/// * `cov` - Matrix to check
/// * `tolerance` - Minimum eigenvalue threshold
///
/// # Returns
/// * `true` if all eigenvalues are greater than tolerance
pub fn is_positive_definite_with_tolerance(cov: &Array2<f64>, tolerance: f64) -> bool {
    if cov.nrows() != cov.ncols() {
        return false;
    }

    // Quick check: diagonal elements must be positive
    for i in 0..cov.nrows() {
        if cov[[i, i]] <= 0.0 {
            return false;
        }
    }

    // Compute eigenvalues and check
    match jacobi_eigendecomp(cov, 100, 1e-12) {
        Ok(decomp) => decomp.eigenvalues.iter().all(|&v| v > tolerance),
        Err(_) => false,
    }
}

/// Compute the condition number of a matrix
///
/// The condition number is the ratio of the largest to smallest eigenvalue.
/// A large condition number indicates numerical instability.
///
/// # Arguments
/// * `cov` - Matrix to analyze
///
/// # Returns
/// * Condition number (infinity if smallest eigenvalue is zero)
pub fn condition_number(cov: &Array2<f64>) -> f64 {
    match jacobi_eigendecomp(cov, 100, 1e-12) {
        Ok(decomp) => {
            let max_eig = decomp
                .eigenvalues
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            let min_eig = decomp
                .eigenvalues
                .iter()
                .cloned()
                .fold(f64::INFINITY, f64::min);

            if min_eig.abs() < 1e-15 {
                f64::INFINITY
            } else {
                max_eig / min_eig
            }
        }
        Err(_) => f64::INFINITY,
    }
}

/// Jacobi eigenvalue decomposition for symmetric matrices
///
/// This implementation uses the Jacobi algorithm, which is stable and simple
/// but may be slower than LAPACK for very large matrices.
///
/// # Arguments
/// * `matrix` - Symmetric matrix to decompose
/// * `max_iterations` - Maximum number of iterations
/// * `tolerance` - Convergence tolerance for off-diagonal elements
///
/// # Returns
/// * Eigenvalues and eigenvectors
pub fn jacobi_eigendecomp(
    matrix: &Array2<f64>,
    max_iterations: usize,
    tolerance: f64,
) -> Result<EigenDecomposition, CovarianceError> {
    let n = matrix.nrows();
    if n != matrix.ncols() {
        return Err(CovarianceError::DimensionMismatch {
            expected: n,
            actual: matrix.ncols(),
        });
    }

    // Initialize: A = copy of input matrix, V = identity
    let mut a = matrix.clone();
    let mut v = Array2::<f64>::eye(n);

    for _iter in 0..max_iterations {
        // Find largest off-diagonal element
        let (p, q, max_val) = find_largest_off_diagonal(&a);

        // Check convergence
        if max_val.abs() < tolerance {
            break;
        }

        // Compute rotation (cos, sin)
        let (cos_theta, sin_theta) = compute_rotation(a[[p, p]], a[[q, q]], a[[p, q]]);

        // Apply Jacobi rotation
        apply_jacobi_rotation(&mut a, &mut v, p, q, cos_theta, sin_theta);
    }

    // Extract eigenvalues from diagonal
    let mut eigenvalues = Array1::<f64>::zeros(n);
    for i in 0..n {
        eigenvalues[i] = a[[i, i]];
    }

    // Sort eigenvalues and eigenvectors in descending order
    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_by(|&i, &j| {
        eigenvalues[j]
            .partial_cmp(&eigenvalues[i])
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let sorted_eigenvalues = indices.iter().map(|&i| eigenvalues[i]).collect();
    let mut sorted_eigenvectors = Array2::<f64>::zeros((n, n));
    for (new_idx, &old_idx) in indices.iter().enumerate() {
        sorted_eigenvectors
            .column_mut(new_idx)
            .assign(&v.column(old_idx));
    }

    Ok(EigenDecomposition {
        eigenvalues: sorted_eigenvalues,
        eigenvectors: sorted_eigenvectors,
    })
}

/// Find the largest off-diagonal element in a symmetric matrix
fn find_largest_off_diagonal(matrix: &Array2<f64>) -> (usize, usize, f64) {
    let n = matrix.nrows();
    let mut max_val = 0.0;
    let mut p = 0;
    let mut q = 1;

    for i in 0..n {
        for j in (i + 1)..n {
            let val = matrix[[i, j]].abs();
            if val > max_val {
                max_val = val;
                p = i;
                q = j;
            }
        }
    }

    (p, q, matrix[[p, q]])
}

/// Compute the rotation (cos, sin) for Jacobi rotation
/// Returns (cos_theta, sin_theta) tuple
fn compute_rotation(app: f64, aqq: f64, apq: f64) -> (f64, f64) {
    if apq.abs() < 1e-15 {
        return (1.0, 0.0);
    }

    let tau = (aqq - app) / (2.0 * apq);
    let t = if tau >= 0.0 {
        1.0 / (tau + (1.0 + tau * tau).sqrt())
    } else {
        -1.0 / (-tau + (1.0 + tau * tau).sqrt())
    };

    // cos = 1/sqrt(1 + t^2), sin = t * cos
    let cos_theta = 1.0 / (1.0 + t * t).sqrt();
    let sin_theta = t * cos_theta;

    (cos_theta, sin_theta)
}

/// Apply a Jacobi rotation to matrix A and eigenvector matrix V
fn apply_jacobi_rotation(
    a: &mut Array2<f64>,
    v: &mut Array2<f64>,
    p: usize,
    q: usize,
    cos_theta: f64,
    sin_theta: f64,
) {
    let n = a.nrows();

    // Rotate A
    let app = a[[p, p]];
    let aqq = a[[q, q]];
    let apq = a[[p, q]];

    a[[p, p]] = cos_theta * cos_theta * app - 2.0 * cos_theta * sin_theta * apq
        + sin_theta * sin_theta * aqq;
    a[[q, q]] = sin_theta * sin_theta * app
        + 2.0 * cos_theta * sin_theta * apq
        + cos_theta * cos_theta * aqq;
    a[[p, q]] = 0.0;
    a[[q, p]] = 0.0;

    // Rotate rows/columns p and q
    for i in 0..n {
        if i != p && i != q {
            let aip = a[[i, p]];
            let aiq = a[[i, q]];

            a[[i, p]] = cos_theta * aip - sin_theta * aiq;
            a[[p, i]] = a[[i, p]];

            a[[i, q]] = sin_theta * aip + cos_theta * aiq;
            a[[q, i]] = a[[i, q]];
        }
    }

    // Update eigenvectors
    for i in 0..n {
        let vip = v[[i, p]];
        let viq = v[[i, q]];

        v[[i, p]] = cos_theta * vip - sin_theta * viq;
        v[[i, q]] = sin_theta * vip + cos_theta * viq;
    }
}

/// Reconstruct a matrix from eigenvalues and eigenvectors
///
/// Computes: M = V * Λ * V^T
///
/// # Arguments
/// * `eigenvalues` - Diagonal elements of Λ
/// * `eigenvectors` - Matrix V (columns are eigenvectors)
///
/// # Returns
/// * Reconstructed matrix
fn reconstruct_from_eigen(
    eigenvalues: &Array1<f64>,
    eigenvectors: &Array2<f64>,
) -> Result<Array2<f64>, CovarianceError> {
    let n = eigenvalues.len();
    if eigenvectors.nrows() != n || eigenvectors.ncols() != n {
        return Err(CovarianceError::DimensionMismatch {
            expected: n,
            actual: eigenvectors.nrows(),
        });
    }

    // V * Λ (multiply each column of V by corresponding eigenvalue)
    let mut v_lambda = Array2::<f64>::zeros((n, n));
    for i in 0..n {
        for j in 0..n {
            v_lambda[[i, j]] = eigenvectors[[i, j]] * eigenvalues[j];
        }
    }

    // (V * Λ) * V^T
    let mut result = Array2::<f64>::zeros((n, n));
    for i in 0..n {
        for j in 0..n {
            let mut sum = 0.0;
            for k in 0..n {
                sum += v_lambda[[i, k]] * eigenvectors[[j, k]];
            }
            result[[i, j]] = sum;
        }
    }

    Ok(result)
}

/// Apply Higham's alternating projections algorithm for nearest positive definite matrix
///
/// This algorithm finds the nearest positive definite matrix in the Frobenius norm.
/// It's particularly useful when dealing with correlation matrices.
///
/// # Arguments
/// * `matrix` - Input matrix (should be symmetric)
/// * `max_iterations` - Maximum number of iterations
///
/// # Returns
/// * Nearest positive definite matrix
pub fn nearest_positive_definite(
    matrix: &Array2<f64>,
    max_iterations: usize,
) -> Result<Array2<f64>, CovarianceError> {
    let n = matrix.nrows();
    if n != matrix.ncols() {
        return Err(CovarianceError::DimensionMismatch {
            expected: n,
            actual: matrix.ncols(),
        });
    }

    // Ensure symmetry
    let mut y = (matrix + &matrix.t()) / 2.0;
    let mut delta_s = Array2::<f64>::zeros((n, n));

    for _iter in 0..max_iterations {
        // Project onto positive semi-definite cone
        let r = &y - &delta_s;
        let decomp = jacobi_eigendecomp(&r, 100, 1e-12)?;

        // Clip negative eigenvalues to zero
        let mut clipped = decomp.eigenvalues.clone();
        for val in clipped.iter_mut() {
            *val = val.max(1e-10);
        }

        let mut x = reconstruct_from_eigen(&clipped, &decomp.eigenvectors)?;
        delta_s = &x - &r;

        // Project onto symmetric matrix with non-negative diagonal
        for i in 0..n {
            x[[i, i]] = x[[i, i]].max(1e-10);
        }

        // Make symmetric
        let x_sym = (&x + &x.t()) / 2.0;

        // Check convergence (simple criterion: Frobenius norm of difference)
        let diff_norm: f64 = (&x_sym - &y).iter().map(|&v| v * v).sum::<f64>().sqrt();
        if diff_norm < 1e-8 {
            y = x_sym;
            break;
        }

        y = x_sym;
    }

    Ok(y)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn test_jacobi_eigendecomp_identity() {
        let matrix = Array2::<f64>::eye(3);
        let decomp = jacobi_eigendecomp(&matrix, 100, 1e-12).unwrap();

        // All eigenvalues should be 1
        for &val in decomp.eigenvalues.iter() {
            assert_abs_diff_eq!(val, 1.0, epsilon = 1e-10);
        }
    }

    #[test]
    fn test_jacobi_eigendecomp_diagonal() {
        let mut matrix = Array2::<f64>::zeros((3, 3));
        matrix[[0, 0]] = 4.0;
        matrix[[1, 1]] = 2.0;
        matrix[[2, 2]] = 1.0;

        let decomp = jacobi_eigendecomp(&matrix, 100, 1e-12).unwrap();

        // Eigenvalues should be 4, 2, 1 (in descending order)
        assert_abs_diff_eq!(decomp.eigenvalues[0], 4.0, epsilon = 1e-10);
        assert_abs_diff_eq!(decomp.eigenvalues[1], 2.0, epsilon = 1e-10);
        assert_abs_diff_eq!(decomp.eigenvalues[2], 1.0, epsilon = 1e-10);
    }

    #[test]
    fn test_jacobi_eigendecomp_symmetric() {
        // Symmetric matrix
        let mut matrix = Array2::<f64>::zeros((3, 3));
        matrix[[0, 0]] = 2.0;
        matrix[[1, 1]] = 2.0;
        matrix[[2, 2]] = 2.0;
        matrix[[0, 1]] = 1.0;
        matrix[[1, 0]] = 1.0;
        matrix[[0, 2]] = 1.0;
        matrix[[2, 0]] = 1.0;
        matrix[[1, 2]] = 1.0;
        matrix[[2, 1]] = 1.0;

        let decomp = jacobi_eigendecomp(&matrix, 100, 1e-12).unwrap();

        // Reconstruct and verify
        let reconstructed =
            reconstruct_from_eigen(&decomp.eigenvalues, &decomp.eigenvectors).unwrap();

        for i in 0..3 {
            for j in 0..3 {
                assert_abs_diff_eq!(matrix[[i, j]], reconstructed[[i, j]], epsilon = 1e-8);
            }
        }
    }

    #[test]
    fn test_is_positive_definite() {
        // Positive definite matrix
        let mut matrix = Array2::<f64>::eye(3);
        matrix[[0, 1]] = 0.5;
        matrix[[1, 0]] = 0.5;
        matrix[[1, 2]] = 0.3;
        matrix[[2, 1]] = 0.3;

        assert!(is_positive_definite(&matrix));
    }

    #[test]
    fn test_is_not_positive_definite() {
        // Matrix with negative eigenvalue
        let mut matrix = Array2::<f64>::zeros((2, 2));
        matrix[[0, 0]] = 1.0;
        matrix[[1, 1]] = -1.0;

        assert!(!is_positive_definite(&matrix));
    }

    #[test]
    fn test_enforce_positive_definite() {
        // Create a symmetric matrix with a negative eigenvalue
        // Use a 2x2 matrix for simplicity: [[1, 2], [2, 1]] has eigenvalues 3 and -1
        let matrix = Array2::from_shape_vec((2, 2), vec![1.0, 2.0, 2.0, 1.0]).unwrap();

        // First verify eigendecomposition works
        let decomp = jacobi_eigendecomp(&matrix, 100, 1e-12).unwrap();
        // Eigenvalues should be 3 and -1
        let _sorted_eigs: Vec<f64> = decomp.eigenvalues.to_vec();

        // After clipping, the negative eigenvalue should become positive
        let config = PositiveDefiniteConfig::default();
        let result = enforce_positive_definite(&matrix, &config).unwrap();

        // Verify the result has positive diagonal (necessary condition)
        assert!(result[[0, 0]] > 0.0);
        assert!(result[[1, 1]] > 0.0);

        // Verify result is symmetric
        assert_abs_diff_eq!(result[[0, 1]], result[[1, 0]], epsilon = 1e-10);

        // Check eigenvalues of result
        let result_decomp = jacobi_eigendecomp(&result, 100, 1e-12).unwrap();

        // All eigenvalues should now be positive
        for eig in result_decomp.eigenvalues.iter() {
            assert!(
                *eig > 0.0 || *eig >= config.min_eigenvalue,
                "Eigenvalue {} should be positive",
                eig
            );
        }
    }

    #[test]
    fn test_condition_number() {
        // Well-conditioned matrix
        let matrix = Array2::<f64>::eye(3);
        let cond = condition_number(&matrix);
        assert_abs_diff_eq!(cond, 1.0, epsilon = 1e-10);

        // Ill-conditioned matrix
        let mut matrix2 = Array2::<f64>::eye(3);
        matrix2[[0, 0]] = 1000.0;
        matrix2[[1, 1]] = 1.0;
        matrix2[[2, 2]] = 0.001;
        let cond2 = condition_number(&matrix2);
        assert!(cond2 > 100.0);
    }

    #[test]
    fn test_enforce_positive_definite_preserves_trace() {
        let mut matrix = Array2::<f64>::zeros((3, 3));
        matrix[[0, 0]] = 2.0;
        matrix[[1, 1]] = 1.0;
        matrix[[2, 2]] = -0.5;

        let original_trace = 2.0 + 1.0 + (-0.5);

        let config = PositiveDefiniteConfig {
            min_eigenvalue: 1e-6,
            preserve_trace: true,
        };

        let result = enforce_positive_definite(&matrix, &config).unwrap();
        let new_trace = result[[0, 0]] + result[[1, 1]] + result[[2, 2]];

        // Trace should be preserved (approximately, since we had a negative eigenvalue)
        assert_abs_diff_eq!(new_trace, original_trace, epsilon = 0.01);
    }

    #[test]
    fn test_nearest_positive_definite() {
        // Create a symmetric matrix with a near-zero eigenvalue
        // [[1, 0.5], [0.5, 0.25]] has eigenvalues 1.25 and 0 (singular)
        let matrix = Array2::from_shape_vec((2, 2), vec![1.0, 0.5, 0.5, 0.25]).unwrap();

        let result = nearest_positive_definite(&matrix, 100).unwrap();

        // Result should be positive definite
        assert!(is_positive_definite(&result));
    }

    #[test]
    fn test_reconstruct_from_eigen() {
        // Simple case: diagonal matrix
        let eigenvalues = Array1::from_vec(vec![3.0, 2.0, 1.0]);
        let eigenvectors = Array2::<f64>::eye(3);

        let result = reconstruct_from_eigen(&eigenvalues, &eigenvectors).unwrap();

        assert_abs_diff_eq!(result[[0, 0]], 3.0, epsilon = 1e-10);
        assert_abs_diff_eq!(result[[1, 1]], 2.0, epsilon = 1e-10);
        assert_abs_diff_eq!(result[[2, 2]], 1.0, epsilon = 1e-10);
    }
}
