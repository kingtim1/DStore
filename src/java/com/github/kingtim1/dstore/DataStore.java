package com.github.kingtim1.dstore;

import java.util.Collection;
import java.util.Set;

/**
 * An interface for incremental data (in the form of scalars and matrices)
 * storage to files, streams, etc.
 * 
 * @author Timothy A. Mann
 * 
 */
public interface DataStore
{
	/**
	 * Checks whether the given string is a valid identifier or not. The
	 * validity of an identifier can mostly be determined by implementing
	 * classes of this interface. However, null cannot be a valid identifier.
	 * 
	 * @param identifier
	 *            a string
	 * @return true if the identifier is valid; otherwise false
	 */
	public boolean isValid(String identifier);
	
	/**
	 * Returns a set of all bound identifiers.
	 * @return a set of all bound identifiers
	 */
	public Set<String> boundIdentifiers();
	
	/**
	 * Removes all bound identifiers from the data store.
	 */
	public void clear();

	/**
	 * Checks whether the identifier is both valid and bound to data.
	 * 
	 * @param identifier
	 *            a string identifier
	 * @return true if the identifier is valid and bound; otherwise false
	 */
	public boolean isBound(String identifier);

	/**
	 * Checks whether the data bound to the specified identifier is a scalar
	 * value.
	 * 
	 * @param identifier
	 *            a string identifier
	 * @return true if the identifier is valid and bound to a scalar value;
	 *         otherwise false
	 */
	public boolean isScalar(String identifier);

	/**
	 * Checks whether the data bound to the specified identifier is a matrix.
	 * 
	 * @param identifier
	 *            a string identifier
	 * @return true if the identifier is valid and bound to a matrix; otherwise
	 *         false
	 */
	public boolean isMatrix(String identifier);

	/**
	 * Removes a bound variable (and its data). If the identifier is not bound
	 * to data, then false is returned.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @return true if the bound variable was successfully removed; otherwise
	 *         false
	 */
	public boolean remove(String identifier);

	/**
	 * Binds a scalar value to an identifier, overwriting any previous binding.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param dscalar
	 *            a scalar value
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid
	 * @throws NullPointerException
	 *             if identifier is null
	 */
	public void bind(String identifier, double dscalar);

	/**
	 * Binds a scalar value to an identifier, overwriting any previous binding.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param nscalar
	 *            a number
	 * @throws IllegalArgumentException
	 *             is the identifier is invalid
	 * @throws NullPointerException
	 *             if identifier or nscalar are null
	 */
	public void bind(String identifier, Number nscalar);

	/**
	 * Binds an integer vector to an identifier, overwriting any previous
	 * binding.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param ivector
	 *            an integer vector
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid
	 * @throws NullPointerException
	 *             if either identifier or ivector are null
	 */
	public void bind(String identifier, int[] ivector);

	/**
	 * Binds a float vector to an identifier, overwriting any previous binding.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param fvector
	 *            a float vector
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid
	 * @throws NullPointerException
	 *             if either identifier or fvector are null
	 */
	public void bind(String identifier, float[] fvector);

	/**
	 * Binds a double vector to an identifier, overwriting any previous binding.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param dvector
	 *            a vector of double floating point values
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid
	 * @throws NullPointerException
	 *             if either identifier or dvector are null
	 */
	public void bind(String identifier, double[] dvector);

	/**
	 * Binds a collection of numbers (treated as a vector) to an identifier,
	 * overwriting any previous binding. The order of the numbers will be the
	 * order that each number is presented by the iterator over the collection.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param cvector
	 *            a collection of double values
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid
	 * @throws NullPointerException
	 *             if either identifier or cvector are null
	 */
	public void bind(String identifier, Collection<? extends Number> cvector);

	/**
	 * Binds a 2-dimensional array of integers (treated as a matrix) to an
	 * identifier, overwriting any previous binding. The first index is treated
	 * as the row index, while the second index is treated as the column index.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param imatrix
	 *            a rectangular matrix of integers
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the imatrix does not have the
	 *             same number of columns in each row
	 * @throws NullPointerException
	 *             if either identifier or imatrix is null
	 */
	public void bind(String identifier, int[][] imatrix);

	/**
	 * Binds a 2-dimensional array of floats (treated as a matrix) to an
	 * identifier, overwriting any previous binding. The first index is treated
	 * as the row index, while the second index is treated as the column index.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param fmatrix
	 *            a rectangular matrix of floats
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the fmatrix does not have the
	 *             same number of columns in each row
	 * @throws NullPointerException
	 *             if identifier or fmatrix are null
	 */
	public void bind(String identifier, float[][] fmatrix);

	/**
	 * Binds a 2-dimensional array of doubles (treated as a matrix) to an
	 * identifier, overwriting any previous binding. The first index is treated
	 * as the row index, while the second index is treated as the column index.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param dmatrix
	 *            a rectangular matrix of doubles
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the dmatrix does not have the
	 *             same number of columns in each row
	 * @throws NullPointerException
	 *             if the identifier or dmatrix are null
	 */
	public void bind(String identifier, double[][] dmatrix);

	/**
	 * Appends an integer array as a row to an existing bound matrix or creates
	 * a new matrix with a single row.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param irow
	 *            an integer array
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the number of elements in
	 *             irow does not match the number of columns of the appended
	 *             matrix
	 * @throws NullPointerException
	 *             if identifier or irow are null
	 */
	public void appendRow(String identifier, int[] irow);

	/**
	 * Appends a float array as a row to an existing bound matrix or creates a
	 * new matrix with a single row.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param frow
	 *            a float array
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the number of elements in
	 *             frow does not match the number of columns of the appended
	 *             matrix
	 * @throws NullPointerException
	 *             if identifier or frow are null
	 */
	public void appendRow(String identifier, float[] frow);

	/**
	 * Appends a double array as a row to an existing bound matrix or creates a
	 * new matrix with a single row.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param drow
	 *            a double array
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the number of elements in the
	 *             drow does not match the number of columns of the appended
	 *             matrix
	 * @throws NullPointerException
	 *             if identifier or drow is null
	 */
	public void appendRow(String identifier, double[] drow);

	/**
	 * Appends a collection of numbers as a row to an existing bound matrix or
	 * creates a new matrix with a single row. The order of the numbers in the
	 * row is determined by the iterator over the collection.
	 * 
	 * @param identifier
	 *            a valid string identifier
	 * @param crow
	 *            a collection of numbers
	 * @throws IllegalArgumentException
	 *             if the identifier is invalid or the number of elements in the
	 *             collection does not match the number of columns in the
	 *             appended matrix
	 * @throws NullPointerException
	 *             if identifier or crow is null
	 */
	public void appendRow(String identifier, Collection<? extends Number> crow);

}
