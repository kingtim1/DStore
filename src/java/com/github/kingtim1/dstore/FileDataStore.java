package com.github.kingtim1.dstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Implements incremental storage of scalar, matrix, and string data to a set of
 * files in a specified directory.
 * 
 * @author Timothy A. Mann
 * 
 */
public class FileDataStore implements DataStore {
	public static final String VALID_ID_REGEX = "[\\p{Alpha}_][\\p{Alnum}_\\.]*";
	public static final String BIND_FILE = "bindings";
	public static final String SCALAR_FILE = "scalars";
	public static final String STRING_DIR = "string_data";
	public static final String MATRIX_FILE = "matrices";
	public static final String MATRIX_DIR = "matrices_data";
	public static final String DESC_FILE = "description.txt";
	public static final String TMP_EXT = ".tmp";
	public static final String PATH_SEPARATOR = System
			.getProperty("file.separator");

	public static final char SPLIT_CHAR = ':';
	public static final String SPLIT_STR = String.valueOf(SPLIT_CHAR);

	public static final Integer SCALAR_TYPE = 1;
	public static final Integer MATRIX_TYPE = 0;
	public static final Integer STRING_TYPE = 2;
	public static final String SCALAR_STR = String.valueOf(SCALAR_TYPE);
	public static final String MATRIX_STR = String.valueOf(MATRIX_TYPE);
	public static final String STRING_STR = String.valueOf(STRING_TYPE);
	public static final String ANY_STR = "\\p{Graph}+";

	public static final int DOUBLE_SIZE = 8;
	public static final int DEFAULT_MAX_MAT_BUFFER_SIZE = 8192;

	private File _dataDir;
	private File _bindFile;
	private File _scalarFile;
	private File _matrixFile;
	private File _descFile;
	private File _matDir;
	private File _strDir;

	private Set<String> _ids;
	private Map<String, Integer> _types;
	private Map<String, Integer> _matrixNumRows;
	private Map<String, Integer> _matrixNumCols;

	private boolean _readOnly;
	private boolean _gzip;
	private int _maxMatBufferSize;

	public FileDataStore(File dataDir) throws IOException {
		this(dataDir, false);
	}

	public FileDataStore(File dataDir, boolean readOnly) throws IOException {
		this(dataDir, readOnly, true, null);
	}

	public FileDataStore(File dataDir, boolean readOnly, boolean gzip,
			Integer maxMatBufferSize) throws IOException {
		if (dataDir == null) {
			throw new NullPointerException(
					"Cannot construct data store with null file.");
		}
		_readOnly = readOnly;
		_gzip = gzip;

		_maxMatBufferSize = DEFAULT_MAX_MAT_BUFFER_SIZE;
		// Only use the specified maxMatBufferSize if it is bigger than the
		// default
		if (maxMatBufferSize != null
				&& maxMatBufferSize > DEFAULT_MAX_MAT_BUFFER_SIZE) {
			_maxMatBufferSize = maxMatBufferSize;
		}

		_dataDir = dataDir;
		createIfNeeded(_dataDir, true, "main");
		String absPath = _dataDir.getAbsolutePath();

		_descFile = new File(absPath + PATH_SEPARATOR + DESC_FILE);
		createIfNeeded(_descFile, false, "description");

		_bindFile = new File(absPath + PATH_SEPARATOR + BIND_FILE);
		createIfNeeded(_bindFile, false, "bindings");

		_scalarFile = new File(absPath + PATH_SEPARATOR + SCALAR_FILE);
		createIfNeeded(_scalarFile, false, "scalar");

		_matrixFile = new File(absPath + PATH_SEPARATOR + MATRIX_FILE);
		createIfNeeded(_matrixFile, false, "matrix size");
		_matDir = new File(absPath + PATH_SEPARATOR + MATRIX_DIR);
		createIfNeeded(_matDir, true, "matrix element");

		_strDir = new File(absPath + PATH_SEPARATOR + STRING_DIR);
		createIfNeeded(_strDir, true, "string");

		_ids = new HashSet<String>();
		_types = new HashMap<String, Integer>();
		populateIdentifiers();

		_matrixNumRows = new HashMap<String, Integer>();
		_matrixNumCols = new HashMap<String, Integer>();
		populateMatrixSizes();
	}

	public boolean isReadOnly() {
		return _readOnly;
	}

	public boolean gzipsMatrices() {
		return _gzip;
	}

	private void createIfNeeded(File file, boolean isSupposedToBeDir,
			String whatTypeOfDataItHolds) throws IOException {
		if (!file.exists()) {
			if (_readOnly) {
				String fileOrDirStr = isSupposedToBeDir ? "directory" : "file";
				throw new IOException(
						"The "
								+ whatTypeOfDataItHolds
								+ " data "
								+ fileOrDirStr
								+ " does not exist and cannot be created in read only mode.");
			}
			if (isSupposedToBeDir) {
				file.mkdirs();
			} else {
				file.createNewFile();
			}
		} else {
			if (isSupposedToBeDir && !file.isDirectory()) {
				throw new IOException("Expected " + file
						+ " to be a directory for " + whatTypeOfDataItHolds
						+ " data. Found a non-directory file instead.");
			}
		}
	}

	/**
	 * Returns the main data directory being used by this FileDataStore
	 * instance.
	 * 
	 * @return the main data directory being used
	 */
	public File dataDir() {
		return _dataDir;
	}

	private void populateIdentifiers() throws IOException {
		FileReader fread = new FileReader(_bindFile);
		BufferedReader bread = new BufferedReader(fread);
		String line = null;
		while ((line = bread.readLine()) != null) {
			String[] tokens = line.split(SPLIT_STR);
			String identifier = tokens[0];
			Integer type = Integer.parseInt(tokens[1]);
			_ids.add(identifier);
			_types.put(identifier, type);
		}
		bread.close();
	}

	private void populateMatrixSizes() throws IOException {
		FileReader fread = new FileReader(_matrixFile);
		BufferedReader bread = new BufferedReader(fread);
		String line = null;
		while ((line = bread.readLine()) != null) {
			String[] tokens = line.split(SPLIT_STR);
			String identifier = tokens[0];
			int numRows = Integer.parseInt(tokens[1]);
			int numCols = Integer.parseInt(tokens[2]);

			_matrixNumRows.put(identifier, numRows);
			_matrixNumCols.put(identifier, numCols);
		}
		bread.close();
	}

	private File matrixDataFile(String identifier) {
		return new File(_matDir.getAbsolutePath() + PATH_SEPARATOR + identifier);
	}

	private File stringDataFile(String identifier) {
		return new File(_strDir.getAbsolutePath() + PATH_SEPARATOR + identifier);
	}

	@Override
	public boolean isValid(String identifier) {
		return identifier.matches(VALID_ID_REGEX);
	}

	@Override
	public Set<String> boundIdentifiers() {
		return new HashSet<String>(_ids);
	}

	/**
	 * Returns a description of this data storage or the empty string if none
	 * exists.
	 * 
	 * @return a description of this data storage
	 */
	public String description() {
		try {
			FileReader freader = new FileReader(_descFile);
			BufferedReader breader = new BufferedReader(freader);
			StringBuilder desc = new StringBuilder();
			String line = breader.readLine();
			while (line != null) {
				desc.append(line + TextFileUtil.NEWLINE);
				line = breader.readLine();
			}
			breader.close();
			return desc.toString();
		} catch (IOException ex) {
			return "";
		}
	}

	/**
	 * Sets the current description of this data store overwriting any previous
	 * description.
	 * 
	 * @param description
	 *            a description of this data store
	 * @throws IOException
	 *             if an I/O error occurs while writing the description to a
	 *             file
	 */
	public void setDescription(String description) throws IOException {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot modify description in read-only mode.");
		}
		FileWriter fwriter = new FileWriter(_descFile, false);
		BufferedWriter bwriter = new BufferedWriter(fwriter);
		bwriter.write(description);
		bwriter.close();
	}

	/**
	 * Appends <code>moreDescription</code> to the current description.
	 * 
	 * @param moreDescription
	 *            string that will augment the current description
	 * @throws IOException
	 *             if an I/O error occurs while writing the description to a
	 *             file
	 */
	public void appendDescription(String moreDescription) throws IOException {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot modify description in read-only mode.");
		}
		FileWriter fwriter = new FileWriter(_descFile, true);
		BufferedWriter bwriter = new BufferedWriter(fwriter);
		bwriter.write(moreDescription);
		bwriter.close();
	}

	@Override
	public void clear() {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot clear data in read-only mode.");
		}

		try {
			for (String id : _ids) {
				if (isMatrix(id)) {
					File matFile = matrixDataFile(id);
					matFile.delete();
				}
				if (isString(id)) {
					File strFile = stringDataFile(id);
					strFile.delete();
				}
			}

			_ids.clear();
			_types.clear();

			_matrixNumRows.clear();
			_matrixNumCols.clear();

			_bindFile.delete();
			_scalarFile.delete();
			_matrixFile.delete();

			_bindFile.createNewFile();
			_scalarFile.createNewFile();
			_matrixFile.createNewFile();
		} catch (IOException ex) {
			throw new DebugException("Failed to clear data from a "
					+ getClass().getName() + " instance.");
		}
	}

	@Override
	public boolean isBound(String identifier) {
		return _ids.contains(identifier);
	}

	@Override
	public boolean isScalar(String identifier) {
		Integer response = _types.get(identifier);
		return SCALAR_TYPE.equals(response);
	}

	@Override
	public boolean isMatrix(String identifier) {
		Integer response = _types.get(identifier);
		if (isBound(identifier) && MATRIX_TYPE.equals(response)) {
			return true;
		} else {
			return false;
		}
	}

	public boolean isString(String identifier) {
		Integer response = _types.get(identifier);
		if (isBound(identifier) && STRING_TYPE.equals(response)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Returns true if this file data store instance is in read only mode. In
	 * read only mode the file data store can only be inspected and cannot be
	 * modified.
	 * 
	 * @return true if in read only mode; otherwise false
	 */
	public boolean readOnly() {
		return _readOnly;
	}

	@Override
	public boolean remove(String identifier) {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot remove bindings while in read only mode.");
		}
		if (isBound(identifier)) {
			if (isScalar(identifier)) {
				return removeScalar(identifier);
			} else if (isMatrix(identifier)) {
				return removeMatrix(identifier);
			} else if (isString(identifier)) {
				return removeString(identifier);
			} else {
				throw new DebugException(
						"Detected identifier that is neither scalar, matrix, or string. This should never happen!");
			}
		}
		return false;
	}

	private boolean removeScalar(String identifier) {

		String deleteBindingStr = identifier + SPLIT_STR + SCALAR_STR;
		String deleteValueStr = identifier + SPLIT_STR + ANY_STR;

		try {
			File tmpBindFile = new File(_bindFile.getAbsolutePath() + TMP_EXT);
			TextFileUtil.deleteLines(_bindFile, tmpBindFile, deleteBindingStr);
			File tmpScalarFile = new File(_scalarFile.getAbsolutePath()
					+ TMP_EXT);
			TextFileUtil
					.deleteLines(_scalarFile, tmpScalarFile, deleteValueStr);

			_ids.remove(identifier);
			_types.remove(identifier);
		} catch (IOException ex) {
			return false;
		}

		return true;
	}

	private boolean removeString(String identifier) {
		String deleteBindingStr = identifier + SPLIT_STR + STRING_STR;
		try {
			File tmpBindFile = new File(_bindFile.getAbsolutePath() + TMP_EXT);
			TextFileUtil.deleteLines(_bindFile, tmpBindFile, deleteBindingStr);

			File strDataFile = stringDataFile(identifier);
			strDataFile.delete();

			_ids.remove(identifier);
			_types.remove(identifier);
		} catch (IOException ex) {
			return false;
		}
		return true;
	}

	private boolean removeMatrix(String identifier) {
		String deleteBindingStr = identifier + SPLIT_STR + MATRIX_STR;
		String deleteMatrixStr = identifier + SPLIT_STR + ANY_STR;

		try {
			File tmpBindFile = new File(_bindFile.getAbsoluteFile() + TMP_EXT);
			TextFileUtil.deleteLines(_bindFile, tmpBindFile, deleteBindingStr);

			File tmpMatrixFile = new File(_matrixFile.getAbsolutePath()
					+ TMP_EXT);
			TextFileUtil.deleteLines(_matrixFile, tmpMatrixFile,
					deleteMatrixStr);

			File matrixDataFile = matrixDataFile(identifier);
			matrixDataFile.delete();

			_ids.remove(identifier);
			_types.remove(identifier);

			_matrixNumRows.remove(identifier);
			_matrixNumCols.remove(identifier);
		} catch (IOException ex) {
			return false;
		}

		return true;
	}

	public void bind(String identifier, String strData) {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot bind data while in read only mode.");
		}
		if (identifier == null) {
			throw new NullPointerException(
					"Cannot create binding with null identifier.");
		}
		if (!isValid(identifier)) {
			throw new IllegalArgumentException("String \"" + identifier
					+ "\" is not a valid identifier.");
		}

		if (isBound(identifier)) {
			remove(identifier);
		}

		try {
			BufferedWriter bindWriter = new BufferedWriter(new FileWriter(
					_bindFile, true));
			bindWriter.append(identifier + SPLIT_STR + STRING_STR
					+ TextFileUtil.NEWLINE);
			bindWriter.close();

			File file = stringDataFile(identifier);
			FileWriter writer = new FileWriter(file);
			BufferedWriter bwriter = new BufferedWriter(writer);
			bwriter.write(strData);
			bwriter.close();

			_ids.add(identifier);
			_types.put(identifier, STRING_TYPE);

		} catch (IOException ex) {
			throw new DebugException(
					"An error occurred while binding a string to an identifier.",
					ex);
		}
	}

	@Override
	public void bind(String identifier, double dscalar) {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot bind data while in read only mode.");
		}
		if (identifier == null) {
			throw new NullPointerException(
					"Cannot create binding with null identifier.");
		}
		if (!isValid(identifier)) {
			throw new IllegalArgumentException("String \"" + identifier
					+ "\" is not a valid identifier.");
		}

		if (isBound(identifier)) {
			remove(identifier);
		}

		try {
			BufferedWriter bindWriter = new BufferedWriter(new FileWriter(
					_bindFile, true));
			bindWriter.append(identifier + SPLIT_STR + SCALAR_STR
					+ TextFileUtil.NEWLINE);
			bindWriter.close();

			BufferedWriter scalarWriter = new BufferedWriter(new FileWriter(
					_scalarFile, true));
			scalarWriter.append(identifier + SPLIT_STR + dscalar
					+ TextFileUtil.NEWLINE);
			scalarWriter.close();

			_ids.add(identifier);
			_types.put(identifier, SCALAR_TYPE);

		} catch (IOException ex) {
			throw new DebugException(
					"An error occurred while binding a scalar to an identifier.",
					ex);
		}
	}

	@Override
	public void bind(String identifier, Number nscalar) {
		bind(identifier, nscalar.doubleValue());
	}

	@Override
	public void bind(String identifier, int[] ivector) {
		bind(identifier, new int[][] { ivector });

	}

	@Override
	public void bind(String identifier, float[] fvector) {
		bind(identifier, new float[][] { fvector });
	}

	@Override
	public void bind(String identifier, double[] dvector) {
		bind(identifier, new double[][] { dvector });
	}

	@Override
	public void bind(String identifier, Collection<? extends Number> cvector) {
		double[] dvector = new double[cvector.size()];
		int i = 0;
		for (Number n : cvector) {
			dvector[i] = n.doubleValue();
			i++;
		}
		bind(identifier, dvector);
	}

	public void bind(String identifier, List<? extends Number> lvector) {
		bind(identifier, new ListArrayWrapper(lvector, 1, lvector.size()));
	}

	private static interface NumericArrayWrapper {
		public double get(int row, int col);

		public int numRows();

		public int numCols();
	}

	private static class ListArrayWrapper implements NumericArrayWrapper {
		private List<? extends Number> _ldata;
		private int _numRows;
		private int _numCols;

		public ListArrayWrapper(List<? extends Number> ldata, int numRows,
				int numCols) {
			_ldata = ldata;
			_numRows = numRows;
			_numCols = numCols;
		}

		public int numRows() {
			return _numRows;
		}

		public int numCols() {
			return _numCols;
		}

		public double get(int row, int col) {
			int index = row * _numCols + col;
			return _ldata.get(index).doubleValue();
		}
	}

	private static class IntArrayWrapper implements NumericArrayWrapper {
		private int[][] _mat;

		public IntArrayWrapper(int[][] mat) {
			_mat = mat;
		}

		@Override
		public double get(int row, int col) {
			return _mat[row][col];
		}

		@Override
		public int numRows() {
			return _mat.length;
		}

		@Override
		public int numCols() {
			return _mat[0].length;
		}
	}

	private static class FloatArrayWrapper implements NumericArrayWrapper {
		private float[][] _mat;

		public FloatArrayWrapper(float[][] mat) {
			_mat = mat;
		}

		@Override
		public double get(int row, int col) {
			return _mat[row][col];
		}

		@Override
		public int numRows() {
			return _mat.length;
		}

		@Override
		public int numCols() {
			return _mat[0].length;
		}
	}

	private static class DoubleArrayWrapper implements NumericArrayWrapper {
		private double[][] _mat;

		public DoubleArrayWrapper(double[][] mat) {
			_mat = mat;
		}

		@Override
		public double get(int row, int col) {
			return _mat[row][col];
		}

		@Override
		public int numRows() {
			return _mat.length;
		}

		@Override
		public int numCols() {
			return _mat[0].length;
		}
	}

	@Override
	public void bind(String identifier, int[][] imatrix) {
		bind(identifier, new IntArrayWrapper(imatrix));
	}

	@Override
	public void bind(String identifier, float[][] fmatrix) {
		bind(identifier, new FloatArrayWrapper(fmatrix));
	}

	@Override
	public void bind(String identifier, double[][] dmatrix) {
		bind(identifier, new DoubleArrayWrapper(dmatrix));
	}

	private void bind(String identifier, NumericArrayWrapper matrix) {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot bind data while in read only mode.");
		}
		if (identifier == null) {
			throw new NullPointerException(
					"Cannot create binding with null identifier.");
		}
		if (!isValid(identifier)) {
			throw new IllegalArgumentException("String \"" + identifier
					+ "\" is not a valid identifier.");
		}

		if (isBound(identifier)) {
			remove(identifier);
		}

		try {
			BufferedWriter bindWriter = new BufferedWriter(new FileWriter(
					_bindFile, true));
			bindWriter.append(identifier + SPLIT_STR + MATRIX_STR
					+ TextFileUtil.NEWLINE);
			bindWriter.close();

			BufferedWriter matrixWriter = new BufferedWriter(new FileWriter(
					_matrixFile, true));
			matrixWriter.append(identifier + SPLIT_STR + matrix.numRows()
					+ SPLIT_STR + matrix.numCols() + TextFileUtil.NEWLINE);
			matrixWriter.close();

			BufferedOutputStream bos = null;
			int maxBuffSize = Math.min(matrix.numRows() * matrix.numCols()
					* DOUBLE_SIZE, _maxMatBufferSize);
			if (_gzip) {
				GZIPOutputStream gzstream = new GZIPOutputStream(
						new FileOutputStream(matrixDataFile(identifier)));
				bos = new BufferedOutputStream(gzstream, maxBuffSize);
			} else {
				FileOutputStream fos = new FileOutputStream(
						matrixDataFile(identifier));
				bos = new BufferedOutputStream(fos, maxBuffSize);
			}
			DataOutputStream matrixDataStream = new DataOutputStream(bos);
			for (int r = 0; r < matrix.numRows(); r++) {
				for (int c = 0; c < matrix.numCols(); c++) {
					matrixDataStream.writeDouble(matrix.get(r, c));
				}
			}
			matrixDataStream.close();

			_ids.add(identifier);
			_types.put(identifier, MATRIX_TYPE);

			_matrixNumRows.put(identifier, matrix.numRows());
			_matrixNumCols.put(identifier, matrix.numCols());

		} catch (IOException ex) {
			throw new DebugException(
					"An error occurred while binding a matrix to an identifier.",
					ex);
		}
	}

	@Override
	public void appendRow(String identifier, int[] irow) {
		appendRow(identifier, new IntArrayWrapper(new int[][] { irow }));
	}

	@Override
	public void appendRow(String identifier, float[] frow) {
		appendRow(identifier, new FloatArrayWrapper(new float[][] { frow }));
	}

	@Override
	public void appendRow(String identifier, double[] drow) {
		appendRow(identifier, new DoubleArrayWrapper(new double[][] { drow }));
	}

	@Override
	public void appendRow(String identifier, Collection<? extends Number> crow) {
		double[] drow = new double[crow.size()];
		int i = 0;
		for (Number n : crow) {
			drow[i] = n.doubleValue();
			i++;
		}
		appendRow(identifier, drow);
	}

	public void appendRow(String identifier, List<? extends Number> lrow) {
		appendRow(identifier, new ListArrayWrapper(lrow, 1, lrow.size()));
	}

	private void appendRow(String identifier, NumericArrayWrapper row) {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot append matrix data while in read only mode.");
		}
		if (isMatrix(identifier)) {

			try {

				BufferedOutputStream bos = null;
				int maxBuffSize = Math.min(row.numRows() * row.numCols()
						* DOUBLE_SIZE, _maxMatBufferSize);
				if (_gzip) {
					GZIPOutputStream gzstream = new GZIPOutputStream(
							new FileOutputStream(matrixDataFile(identifier),
									true));
					bos = new BufferedOutputStream(gzstream, maxBuffSize);
				} else {
					FileOutputStream fos = new FileOutputStream(
							matrixDataFile(identifier), true);
					bos = new BufferedOutputStream(fos, maxBuffSize);
				}
				DataOutputStream matrixDataStream = new DataOutputStream(bos);

				for (int r = 0; r < row.numRows(); r++) {
					for (int c = 0; c < row.numCols(); c++) {
						matrixDataStream.writeDouble(row.get(r, c));
					}
				}
				matrixDataStream.close();

				int nrows = matrixNumRows(identifier);
				int ncols = matrixNumCols(identifier);
				String oldEntry = identifier + SPLIT_STR + nrows + SPLIT_STR
						+ ncols;
				String newEntry = identifier + SPLIT_STR
						+ (nrows + row.numRows()) + SPLIT_STR + ncols;
				TextFileUtil.replaceLine(oldEntry, newEntry,
						_matrixFile.getAbsolutePath());

				_matrixNumRows.put(identifier, nrows + row.numRows());
			} catch (IOException ex) {
				throw new DebugException(
						"An error occurred while appending a row to a bound matrix.",
						ex);
			}

		} else {
			bind(identifier, row);
		}
	}

	public int matrixNumRows(String identifier) {
		if (isMatrix(identifier)) {
			return _matrixNumRows.get(identifier);
		} else {
			throw new IllegalArgumentException("Identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}

	}

	public int matrixNumCols(String identifier) {
		if (isMatrix(identifier)) {
			return _matrixNumCols.get(identifier);
		} else {
			throw new IllegalArgumentException("Identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}
	}

	public String getString(String identifier) throws IOException {
		if (isString(identifier)) {
			File file = stringDataFile(identifier);
			BufferedReader stringReader = new BufferedReader(new FileReader(
					file));
			StringBuilder str = new StringBuilder();

			String line = null;
			while ((line = stringReader.readLine()) != null) {
				str.append(line);
			}
			stringReader.close();

			return str.toString();
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a string.");
		}
	}

	public double getScalar(String identifier) throws IOException {
		if (isScalar(identifier)) {
			BufferedReader scalarReader = new BufferedReader(new FileReader(
					_scalarFile));
			String line = null;
			while ((line = scalarReader.readLine()) != null) {
				if (line.matches(identifier + SPLIT_STR + ANY_STR)) {
					String[] tokens = line.split(SPLIT_STR);
					scalarReader.close();
					return Double.parseDouble(tokens[1]);
				}
			}
			scalarReader.close();
			throw new DebugException(
					"Reach end of scalar file without finding identifier \""
							+ identifier + "\"");
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a scalar.");
		}
	}

	public double[][] getMatrix(String identifier) throws IOException {
		if (isMatrix(identifier)) {

			int nrows = matrixNumRows(identifier);
			int ncols = matrixNumCols(identifier);
			double[][] matrix = new double[nrows][ncols];

			GZIPInputStream gzstream = new GZIPInputStream(new FileInputStream(
					matrixDataFile(identifier)));
			BufferedInputStream bis = new BufferedInputStream(gzstream);
			DataInputStream matrixDataStream = new DataInputStream(bis);
			for (int r = 0; r < nrows; r++) {
				for (int c = 0; c < ncols; c++) {
					matrix[r][c] = matrixDataStream.readDouble();
				}
			}
			matrixDataStream.close();

			return matrix;
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}
	}

	public double[][] getMatrixSubsampleColumns(String identifier,
			double proportion) throws IOException {
		if (proportion <= 0) {
			throw new IllegalArgumentException(
					"Subsample proportion must be in (0, 1].");
		}
		if (isMatrix(identifier)) {

			int nrows = matrixNumRows(identifier);
			int ncols = matrixNumCols(identifier);

			int subNCols = (int) (proportion * ncols);
			double[][] matrix = new double[nrows][subNCols];
			if (subNCols == 0) {
				return matrix;
			}
			int skip = ((ncols / subNCols) - 1);
			int remaining = ncols - ((subNCols - 1) * (skip + 1)) - 1;

			GZIPInputStream gzstream = new GZIPInputStream(new FileInputStream(
					matrixDataFile(identifier)));
			BufferedInputStream bis = new BufferedInputStream(gzstream);
			DataInputStream matrixDataStream = new DataInputStream(bis);
			for (int r = 0; r < nrows; r++) {
				for (int c = 0; c < subNCols; c++) {
					matrix[r][c] = matrixDataStream.readDouble();
					if (c < subNCols - 1) {
						matrixDataStream.skip(skip * (Double.SIZE / 8));
					} else {
						matrixDataStream.skip(remaining * (Double.SIZE / 8));
					}
				}
			}
			matrixDataStream.close();

			return matrix;
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}
	}

	public double[] getMatrixRowSum(String identifier) throws IOException {
		if (isMatrix(identifier)) {
			int nrows = matrixNumRows(identifier);
			int ncols = matrixNumCols(identifier);
			double[] rowSum = new double[ncols];

			GZIPInputStream gzstream = new GZIPInputStream(new FileInputStream(
					matrixDataFile(identifier)));
			BufferedInputStream bis = new BufferedInputStream(gzstream);
			DataInputStream matrixDataStream = new DataInputStream(bis);
			for (int r = 0; r < nrows; r++) {
				for (int c = 0; c < ncols; c++) {
					rowSum[c] += matrixDataStream.readDouble();
				}
			}
			matrixDataStream.close();
			return rowSum;
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}
	}

	public double[] getMatrixRowMean(String identifier) throws IOException {
		double[] rowSum = getMatrixRowSum(identifier);
		double[] rowMean = rowSum;
		int nrows = matrixNumRows(identifier);
		for (int c = 0; c < rowSum.length; c++) {
			rowMean[c] = rowSum[c] / nrows;
		}
		return rowMean;
	}

	public double[] getMatrixRowVariance(String identifier) throws IOException {
		if (isMatrix(identifier)) {

			int nrows = matrixNumRows(identifier);
			int ncols = matrixNumCols(identifier);
			double[] rowSumSq = new double[ncols];
			double[] rowSum = new double[ncols];

			GZIPInputStream gzstream = new GZIPInputStream(new FileInputStream(
					matrixDataFile(identifier)));
			BufferedInputStream bis = new BufferedInputStream(gzstream);
			DataInputStream matrixDataStream = new DataInputStream(bis);
			for (int r = 0; r < nrows; r++) {
				for (int c = 0; c < ncols; c++) {
					double val = matrixDataStream.readDouble();
					rowSumSq[c] += (val * val);
					rowSum[c] += val;
				}
			}
			matrixDataStream.close();
			double[] rowVar = rowSumSq;
			double unbias = (nrows / (nrows - 1));
			for (int i = 0; i < rowVar.length; i++) {
				rowVar[i] = unbias
						* ((rowSumSq[i] / nrows) - Math.pow(rowSum[i] / nrows,
								2));
			}
			return rowVar;
		} else {
			throw new IllegalArgumentException("The identifier \"" + identifier
					+ "\" is not bound to a matrix.");
		}
	}

	public double[] getMatrixRowStd(String identifier) throws IOException {
		double[] rowVar = getMatrixRowVariance(identifier);
		double[] rowStd = rowVar;
		for (int i = 0; i < rowVar.length; i++) {
			rowStd[i] = Math.sqrt(rowVar[i]);
		}
		return rowStd;
	}

	/**
	 * Merges the bindings from another data storage instance with this
	 * instance. For scalar entries, the value from other is recorded if the
	 * identifier is not already in use. For matrix entries, the matrix is
	 * recorded if the binding is not in use. If the binding is in use by a
	 * scalar, then nothing is overwritten. If the binding is in use by a
	 * matrix, then this function attempts to append the rows of the other data
	 * stores matrix to the existing matrix.
	 * 
	 * @param other
	 *            another file data store object
	 * @return the set of identifiers from <code>other</code> that could not be
	 *         merged
	 */
	public Set<String> merge(FileDataStore other) throws IOException {
		if (_readOnly) {
			throw new IllegalStateException(
					"Cannot merge with other because this "
							+ getClass().getSimpleName()
							+ " is in read-only mode.");
		}
		Set<String> notMerged = new HashSet<String>();
		Set<String> ids = other.boundIdentifiers();
		for (String id : ids) {
			if (!this.isBound(id)) {
				if (other.isScalar(id)) {
					this.bind(id, other.getScalar(id));
				} else if (other.isMatrix(id)) {
					this.bind(id, other.getMatrix(id));
				} else {
					this.bind(id, other.getString(id));
				}
			} else {
				if (this.isMatrix(id)) {
					double[][] omat = other.getMatrix(id);
					try {
						for (double[] omatRow : omat) {
							this.appendRow(id, omatRow);
						}
					} catch (DebugException ex) {
						notMerged.add(id);
					}
				} else {
					notMerged.add(id);
				}
			}
		}
		return notMerged;
	}

}
