require('dotenv').config();
const express = require("express");
const cors = require("cors");
const pool = require("./db");
const bodyParser = require('body-parser');

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// Configure bodyParser with appropriate limits
app.use(bodyParser.json({ limit: '1000mb' }));
app.use(bodyParser.urlencoded({ limit: '1000mb', extended: true }));
app.use(bodyParser.raw({ limit: '1000mb' }));



app.use((req, res, next)=>{
    console.log(req.method, req.url, req.body);
    next();
})

app.get('/', (req, res) => {
  res.send('API is running!');
});
// Enhanced error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({ error: "Internal server error" });
});

/**
 * @api {get} /accounts Get All Bank Accounts with Statements
 * @apiName GetAccounts
 * @apiGroup Accounts
 * @apiVersion 1.0.0
 * 
 * @apiDescription Retrieves all bank accounts with their associated statements from the COMPANY.V_KTO_KONTO_AUSZUG view.
 * Returns comprehensive account information including holder details, account details, and statement information.
 * Results are sorted by year (descending), holder name, IBAN, account type, and month (descending).
 * 
 * @apiSuccess {Object[]} accounts               Array of account objects
 * @apiSuccess {Number}   accounts.holder_id     Account holder ID (FK_KON_OWNER)
 * @apiSuccess {String}   accounts.holder_name   Formatted holder name "Lastname, Firstname"
 * @apiSuccess {Number}   accounts.account_id    Bank account primary key (PK_KTO_BANKKONTO)
 * @apiSuccess {String}   accounts.account_name  IBAN number
 * @apiSuccess {String}   accounts.account_number Bank name
 * @apiSuccess {String}   accounts.account_type  Account type description (BEZ)
 * @apiSuccess {Number}   accounts.year_id       Year of statement (JAHR)
 * @apiSuccess {Number}   accounts.statement_id  Statement primary key (PK_KTO_KONTO_AUSZUG)
 * @apiSuccess {Number}   accounts.mt            Month of statement (MONAT)
 * @apiSuccess {Date}     accounts.statement_date Statement start date (ANFANGSDATUM)
 * @apiSuccess {String}   accounts.statement_name Bank name (repeated from account_number)
 * @apiSuccess {Number}   accounts.FINAL_CNT     Final transaction count
 * @apiSuccess {Number}   accounts.FINAL_AMOUNT  Final amount
 * @apiSuccess {Number}   accounts.ANFANGSBETRAG Starting balance
 * @apiSuccess {Number}   accounts.ENDBETRAG     Ending balance
 * @apiSuccess {Number}   accounts.FK_KTO_KONTO_AUSZUG Foreign key reference
 * 
 * @apiError (500) ServerError Database operation failed
 * 
 * @apiExample {curl} Example usage:
 *   curl -X GET http://localhost:5000/accounts
 * 
 * @apiSampleRequest http://localhost:5000/accounts
 */

//1 -  Get all accounts
app.get("/accounts", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        "FK_KON_OWNER" AS holder_id,
                        "OWN1_NACHNAME" || ', ' || "OWN1_VORNAME" AS holder_name,
                        "PK_KTO_BANKKONTO" AS account_id,
                        "IBAN" AS account_name,
                        "BANK" account_number,
                       	"BEZ" AS account_type,
                       	"JAHR" AS year_id,
                        "JAHR" yr,
                        "PK_KTO_KONTO_AUSZUG" AS statement_id,
                        "MONAT" mt,
                        "ANFANGSDATUM" statement_date,
                        "BANK" AS statement_name,
                	"FINAL_CNT",
                	"FINAL_AMOUNT",
                        "ANFANGSBETRAG",
                        "ENDBETRAG", 
                        "FK_KTO_KONTO_AUSZUG"

                    FROM 
                        "COMPANY"."V_KTO_KONTO_AUSZUG" 
                    ORDER BY 
			"JAHR" desc,
  			2,
			"IBAN",
            		 "PK_KTO_BANKKONTO",           
                         "BEZ",
                        
                          
                         "MONAT" DESC;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

//1 -  Get all accounts
app.get("/transactions", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                    zus.*, 
                    pay."TRANSAKTIONSCODE",
                    pay."RECHNUNGSNUMMER"
                    FROM 
                        "COMPANY"."V_KTO_KONTEN_ZUS" zus
                      left Join "COMPANY"."T_KTO_PAYPAL" pay on zus."FK_MAIN_KEY" = pay."FK_MAIN_KEY"
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

//1 -  Get all accounts
app.get("/belege", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                   *

                    FROM 
                        "COMPANY"."V_INP_BELEGE_ALL1" 
                   ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts with pagination
app.get("/belege1", async (req, res) => {
    try {
        // Get pagination parameters from query string, with defaults
        const page = parseInt(req.query.page) || 1;
        const pageSize = parseInt(req.query.pageSize) || 10;
        
        // Calculate offset for pagination
        const offset = (page - 1) * pageSize;
        
        // Get total count of records for pagination metadata
        const countQuery = `SELECT COUNT(*) FROM "COMPANY"."V_INP_BELEGE_ALL1"`;
        const countResult = await pool.query(countQuery);
        const totalCount = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalCount / pageSize);

        // Get paginated data
        const result = await pool.query(
            `SELECT * FROM "COMPANY"."V_INP_BELEGE_ALL1" 
             ORDER BY "BELEGDATUM"  -- You should specify an ORDER BY for consistent pagination
             LIMIT $1 OFFSET $2`,
            [pageSize, offset]
        );
        
        res.json({
            data: result.rows,
            pagination: {
                page,
                pageSize,
                totalCount,
                totalPages,
                hasNextPage: page < totalPages,
                hasPreviousPage: page > 1
            }
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/inventare", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."V_INV_INVENTARE_1" 
                   
		;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/persons", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."T_KON_PERSON" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/persons/bild", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."V_KON_PERSON_BILD" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/adresse", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."V_ADR_ADRESSE" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/location", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."V_LOC_LOCATION" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/ahn", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        *

                    FROM 
                        "COMPANY"."T_AHN_AHNENTAFEL" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts
app.get("/cnt_images", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        count(*)

                    FROM 
                        "COMPANY"."IMP_IMAGES" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});



// Get all stimmzettel with pagination
app.get("/stimmzettel", async (req, res) => {
    try {
        // Parse pagination parameters from query string
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 20;
        const offset = (page - 1) * limit;
        
        // Get total count of records
        const countResult = await pool.query(
            `SELECT COUNT(*) as total FROM "COMPANY"."T_WAHL_STIMM_ZETTEL"`
        );
        const total = parseInt(countResult.rows[0].total);
        
        // Get paginated data
        const result = await pool.query(
            `SELECT *
             FROM "COMPANY"."T_WAHL_STIMM_ZETTEL"
             ORDER BY "PK_WAHL_STIMM_ZETTEL"
             LIMIT $1 OFFSET $2`,
            [limit, offset]
        );
        
        // Calculate pagination metadata
        const totalPages = Math.ceil(total / limit);
        
        res.json({
            data: result.rows,
            pagination: {
                total,
                totalPages,
                currentPage: page,
                limit,
                hasNextPage: page < totalPages,
                hasPreviousPage: page > 1
            }
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch stimmzettel",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/person/bild/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
               *
             FROM "COMPANY"."V_KON_PERSON_BILD" 
             WHERE "FK_KON_PERSON" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});





//app.use(cors());

// Increase payload size limit for base64 encoded images
/*app.use(bodyParser.json({ limit: '2000mb' }));
app.use(bodyParser.urlencoded({ limit: '2000mb', extended: true }));
app.use(bodyParser.raw({ limit: '2000mb' }));

app.use(express.json({ limit: '2000mb' }));
app.use(express.urlencoded({ limit: '2000mb', extended: true }));
*/
// Handle file upload with binary storage
app.post("/person/bild/ins_old", async (req, res) => {
    try {
        const { description, filename, image } = req.body;

        // Validate input fields
        if (!image || !filename) {
            return res.status(400).json({ 
                error: "Image data and filename are required",
                received: { 
                    hasImage: !!image, 
                    hasFilename: !!filename 
                }
            });
        }

        // Validate base64 data format
        const base64Regex = /^data:image\/(png|jpeg|jpg|gif|webp);base64,/;
        if (!base64Regex.test(image)) {
            return res.status(400).json({ 
                error: "Invalid image format. Supported formats: PNG, JPEG, JPG, GIF, WEBP" 
            });
        }

        // Extract metadata and pure base64 data
        const matches = image.match(base64Regex);
        const imageType = matches[1];
        const base64Data = image.replace(base64Regex, '');

        // Verify base64 data is valid
        try {
            Buffer.from(base64Data, 'base64');
        } catch (error) {
            return res.status(400).json({ error: "Invalid base64 data" });
        }

        // Optional: Implement image compression
        let processedImage;
        try {
            const imageBuffer = Buffer.from(base64Data, 'base64');
            processedImage = await sharp(imageBuffer)
                .resize(1024, 1024, { 
                    fit: 'inside',
                    withoutEnlargement: true 
                })
                .jpeg({ quality: 80, progressive: true })
                .toBuffer();
        } catch (error) {
            console.error("Image processing error:", error);
            processedImage = Buffer.from(base64Data, 'base64');
        }

        // Database operation
        const result = await pool.query(
            `INSERT INTO "COMPANY"."T_BILD_BILDER" 
             ("KLASSIFIKATION_1", "FILENAME") 
             VALUES ($1, $2) 
             RETURNING *`,
            [description || '', filename]
        );

        res.status(201).json({ 
            success: true,
            message: "Image added successfully", 
            data: {
                id: result.rows[0].PK_BILD_BILDER,
                filename: result.rows[0].FILENAME,
                size: processedImage.length,
                type: imageType
            }
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            success: false,
            error: "Failed to insert data",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});

app.post("/person/bild/ins", async (req, res) => {
    try {
        const { description, title, category } = req.body;
        const filename = req.body.files[0]["name"];
        const image = req.body.files[0]["data"];

        // Validate input fields
        if (!image || !filename) {
            return res.status(400).json({ 
                error: "Image data and filename are required",
                received: { 
                    hasImage: !!image, 
                    hasFilename: !!filename 
                }
            });
        }

        // Validate base64 data
        if (!/^data:image\/(png|jpeg|jpg|gif);base64,/.test(image)) {
            return res.status(400).json({ 
                error: "Invalid image format. Must be base64 encoded image" 
            });
        }

        // Extract pure base64 data
        const base64Data = image.replace(/^data:image\/\w+;base64,/, '');
        
        // Verify base64 data
        if (Buffer.from(base64Data, 'base64').toString('base64') !== base64Data) {
            return res.status(400).json({ error: "Invalid base64 data" });
        }

        // Check file size (5MB limit)
        const fileSize = Math.ceil((base64Data.length * 3) / 4);
        if (fileSize > 5 * 1024 * 1024) {
            return res.status(400).json({ 
                error: "File size too large. Maximum size is 5MB" 
            });
        }

        // Database operation
        const result = await pool.query(
            `INSERT INTO "COMPANY"."T_BILD_BILDER" 
             ("FILECONTENT", "FILENAME", "KLASSIFIKATION_1") 
             VALUES ($1, $2, $3) 
             RETURNING *`,
            [base64Data, filename, description || '']
        );

        res.status(201).json({ 
            success: true,
            message: "Image added successfully", 
            data: {
                id: result.rows[0].id,
                filename: result.rows[0].filename,
                size: fileSize
            }
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            success: false,
            error: "Failed to insert data",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});

app.post("/document/insert", async (req, res) => {
    try {
        const { fk_main_key, fk_abl_ordner_page } = req.body;

        // Validate required field
        if (!fk_main_key) {
            return res.status(400).json({
                error: "FK_MAIN_KEY is required",
                received: req.body
            });
        }

        // Set default value if fk_abl_ordner_page is not provided
        const folderPage = fk_abl_ordner_page || 3071;

        // Database operation to insert document
        const result = await pool.query(
            `INSERT INTO "COMPANY"."T_INP_BELEGE_ALL" 
             ("BELEGDATUM", "BEZEICHNUNG", "BRUTTO_BETRAG", "FK_ABL_ORDNER_PAGE")
             SELECT "BUCHUNGSTAG", "BUCHUNGSTEXT", "BETRAG", $2
             FROM "COMPANY"."V_KTO_KONTEN_ZUS"
             WHERE "FK_MAIN_KEY" = $1
             RETURNING "PK_INP_BELEGE_ALL"`,
            [fk_main_key, folderPage]  // Now using both parameters
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                error: "No matching record found in T_KTO_GIROKONTO",
                details: `No record with FK_MAIN_KEY = ${fk_main_key}`
            });
        }

        // Return just the primary key
        res.status(201).json({
            success: true,
            documentId: result.rows[0].PK_INP_BELEGE_ALL
        });

    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({
            success: false,
            error: "Failed to insert document",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});


app.post("/document/insert1", async (req, res) => {
    try {
        const { belegdatum, bezeichnung,  fk_abl_ordner_page, brutto_betrag, belegnummer } = req.body;


        // Set default value if fk_abl_ordner_page is not provided
        const folderPage = fk_abl_ordner_page || 3071;

        // Database operation to insert document
        const result = await pool.query(
            ` 	
 	INSERT INTO "COMPANY"."T_INP_BELEGE_ALL" (
    "BELEGDATUM", 
    "BEZEICHNUNG", 
    "FK_ABL_ORDNER_PAGE",
    "BRUTTO_BETRAG",
    "BELEGNUMMER",
    "JAHR"
) 
VALUES (
    TO_DATE($1, 'DD.MM.YYYY'),
    $2,
$3,
   $4,
    $5,
    substr(TO_DATE($1, 'DD.MM.YYYY')::text,1,4)::double precision
)
RETURNING "PK_INP_BELEGE_ALL"`,
            [belegdatum, bezeichnung,  folderPage, brutto_betrag, belegnummer]  // Now using both parameters
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                error: "No matching record found in T_KTO_GIROKONTO",
                details: `No record with FK_MAIN_KEY = ${fk_main_key}`
            });
        }

        // Return just the primary key
        res.status(201).json({
            success: true,
            documentId: result.rows[0].PK_INP_BELEGE_ALL
        });

    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({
            success: false,
            error: "Failed to insert document",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});



// Insert records endpoint
app.post('/insert-girokonto-records', async (req, res) => {
  try {
    const client = await pool.connect();
    
    // Begin transaction
    await client.query('BEGIN');
    
    // Insert records with transformation
    const insertResult = await client.query(`
      INSERT INTO "COMPANY"."T_KTO_GIROKONTO" (
        "BUCHUNGSTAG",
        "BUCHUNGSTEXT",
        "BUCHUNGSTEXT1",
        "BETRAG",
        "WAEHRUNG",
        "FK_MAIN_KEY"
      ) 
      SELECT 
        TO_DATE(buchungsdatum, 'DD.MM.YYYY'),
        description,
        details,
        CAST(REPLACE(REPLACE(amount, '.', ''), ',', '.') AS DOUBLE PRECISION),
        'EUR',
        nextval('"COMPANY"."KTO_KONTO_SEQ"') 
      FROM "COMPANY".T_IMP_KTO_GIROKONTO1
      RETURNING "FK_MAIN_KEY"
    `);
    

    
    // Commit transaction
    await client.query('COMMIT');
    
    res.json({
      status: 'success',
      message: `Inserted ${insertResult.rowCount} records`,
      insertedRecords: insertResult.rows
    });
    
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Database error:', err);
    res.status(500).json({
      status: 'error',
      message: 'Failed to insert records',
      error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  } finally {
    if (client) client.release();
  }
});


/**
 * Update FK_MAIN_KEY for records where it's null
 */
app.post('/update-main-keys', async (req, res) => {
  let client;
  try {
    client = await pool.connect();
    
    // Begin transaction
    await client.query('BEGIN');
    
    // Execute the update and return the assigned keys
    const result = await client.query(`
      UPDATE "COMPANY"."T_KTO_GIROKONTO" 
      SET "FK_MAIN_KEY" = nextval('"COMPANY"."KTO_KONTO_SEQ"') 
      WHERE "FK_MAIN_KEY" IS NULL 
      RETURNING "FK_MAIN_KEY"
    `);
    
    // Commit transaction
    await client.query('COMMIT');
    
    res.json({
      status: 'success',
      message: `Updated ${result.rowCount} records`,
      assignedKeys: result.rows.map(row => row.FK_MAIN_KEY)
    });
    
  } catch (err) {
    // Rollback on error
    if (client) await client.query('ROLLBACK');
    console.error('Database error:', err);
    
    res.status(500).json({
      status: 'error',
      message: 'Failed to update records',
      error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  } finally {
    if (client) client.release();
  }
});

app.post('/update-image-document', async (req, res) => {
  const { documentId, relationshipId } = req.body;
  let client;

  // Validate input
  if (!documentId || !relationshipId) {
    return res.status(400).json({
      status: 'error',
      message: 'Both documentId and relationshipId are required'
    });
  }

  try {
    client = await pool.connect();
    
    // Begin transaction
    await client.query('BEGIN');
    
    // 1. First verify the relationship exists
    const checkQuery = `
      SELECT "PK_REL_INP_INP_BELEGE_ALL_BILD_BILDER" 
      FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" 
      WHERE "PK_REL_INP_INP_BELEGE_ALL_BILD_BILDER" = $1
    `;
    const checkResult = await client.query(checkQuery, [relationshipId]);
    
    if (checkResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({
        status: 'error',
        message: 'Relationship record not found'
      });
    }
    
    // 2. Verify the document exists
    const docCheckQuery = `
      SELECT "PK_INP_BELEGE_ALL" 
      FROM "COMPANY"."T_INP_BELEGE_ALL" 
      WHERE "PK_INP_BELEGE_ALL" = $1
    `;
    const docCheckResult = await client.query(docCheckQuery, [documentId]);
    
    if (docCheckResult.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({
        status: 'error',
        message: 'Document record not found'
      });
    }
    
    // 3. Perform the update
    const updateQuery = `
      UPDATE "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" 
      SET "FK_INP_BELEGE_ALL" = $1 
      WHERE "PK_REL_INP_INP_BELEGE_ALL_BILD_BILDER" = $2
      RETURNING *
    `;
    const updateResult = await client.query(updateQuery, [documentId, relationshipId]);
    
    // Commit transaction
    await client.query('COMMIT');
    
    res.json({
      status: 'success',
      message: 'Image-document relationship updated successfully',
      updatedRecord: updateResult.rows[0]
    });
    
  } catch (err) {
    // Rollback on error
    if (client) await client.query('ROLLBACK');
    console.error('Database error:', err);
    
    res.status(500).json({
      status: 'error',
      message: 'Failed to update image-document relationship',
      error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
  } finally {
    if (client) client.release();
  }
});



app.post("/document/add_rel", async (req, res) => {
    try {
        const { fk_main_key, fk_inp_belege_all } = req.body;

        // Validate required field
        if (!fk_main_key) {
            return res.status(400).json({
                error: "FK_MAIN_KEY is required",
                received: req.body
            });
        }

 

        // Database operation to insert document
        const result = await pool.query(
            ` insert into "COMPANY"."T_REL_LEX_KTO_BEL" ("FK_MAIN_KEY", "FK_INP_BELEGE_ALL")
 select  	 	 		 	  	  	 	 	 	 	 	 	 	 	  	$1,$2`,
            [fk_main_key, fk_inp_belege_all]  // Now using both parameters
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                error: "No matching record found in T_KTO_GIROKONTO",
                details: `No record with FK_MAIN_KEY = ${fk_main_key}`
            });
        }

        // Return just the primary key
        res.status(201).json({
            success: true,
         
        });

    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({
            success: false,
            error: "Failed to insert document",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});

/**
 * Insert relations between girokonto and konto auszug
 */
app.post('/create-relations', async (req, res) => {
  let client;
  try {
    const { keys, fk_konto_auszug, seite } = req.body;

    // Validate input
    if (!keys || !Array.isArray(keys) || keys.length === 0) {
      return res.status(400).json({
        status: 'error',
        message: 'Array of FK_MAIN_KEY values is required',
        example: { "keys": [63614, 63615], "fk_konto_auszug": 117 }
      });
    }

    client = await pool.connect();
    await client.query('BEGIN');

    // Insert relations
    const result = await client.query(`
      INSERT INTO "COMPANY"."T_REL_KTO_KONTO_AUSZUG_GIR" 
        ("FK_MAIN_KEY", "FK_KTO_KONTO_AUSZUG", "SEITE") 
      SELECT "FK_MAIN_KEY", $1, $3
      FROM "COMPANY"."T_KTO_GIROKONTO"
      WHERE "FK_MAIN_KEY" = ANY($2)
      RETURNING "FK_MAIN_KEY"
    `, [fk_konto_auszug, keys, seite]);

    await client.query('COMMIT');

    res.json({
      status: 'success',
      message: `Created ${result.rowCount} relations`,
      fk_konto_auszug: fk_konto_auszug,
      related_keys: result.rows.map(row => row.FK_MAIN_KEY)
    });

  } catch (err) {
    if (client) {
      await client.query('ROLLBACK');
      client.release();
    }
    console.error('Database error:', err);
    
    res.status(500).json({
      status: 'error',
      message: 'Failed to create relations',
      error: process.env.NODE_ENV === 'development' ? {
        message: err.message,
        detail: err.detail,
        hint: err.hint
      } : undefined
    });
  } finally {
    if (client) client.release();
  }
});


app.post("/document/add_bild_rel", async (req, res) => {
    try {
        const {fk_inp_belege_all, fk_bild_bilder } = req.body;

        // Validate required field
        if (!fk_inp_belege_all) {
            return res.status(400).json({
                error: "FK_INP_BELEGE_ALL is required",
                received: req.body
            });
        }

 

        // Database operation to insert document
        const result = await pool.query(
            `  insert into "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" ( "FK_INP_BELEGE_ALL", "FK_BILD_BILDER")
select  		 		 	  	  	 	 	 	 	 	 	 	 	  	$1,$2`,
            [ fk_inp_belege_all, fk_bild_bilder]  // Now using both parameters
        );

        if (result.rows.length === 0) {
            return res.status(404).json({
                error: "No input available",
                details: `No record with FK_INP_BELEGE_ALL = ${fk_inp_belege_all}`
            });
        }

        // Return just the primary key
        res.status(201).json({
            success: true,
         
        });

    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({
            success: false,
            error: "Failed to insert document",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});




app.put("/accounts/final/:id", async (req, res) => {
    try {
        const { id } = req.params;
        const { final_cnt, final_amount } = req.body;

        // Validate input fields
        if (!id || isNaN(id) || final_cnt === undefined || final_amount === undefined) {
            return res.status(400).json({ error: "Invalid input data" });
        }

        // Convert to numbers explicitly
        const finalCnt = Number(final_cnt);
        const finalAmount = Number(final_amount);

        // Additional number validation
        if (isNaN(finalCnt) || isNaN(finalAmount)) {
            return res.status(400).json({ error: "Count and amount must be valid numbers" });
        }

        const result = await pool.query(
            `UPDATE "COMPANY"."T_KTO_KONTO_AUSZUG" 
             SET "FINAL_CNT" = $1, "FINAL_AMOUNT" = $2 
             WHERE "PK_KTO_KONTO_AUSZUG" = $3 
             RETURNING *`,
            [finalCnt, finalAmount, id]  // Use the converted numbers
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Record not found or no changes made." });
        }

        res.status(200).json({ 
            message: "Account data updated successfully", 
            data: result.rows[0] 
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to update data",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/person/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
               *
             FROM "COMPANY"."T_KON_PERSON" 
             WHERE "PK_KON_PERSON" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/ahn/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
               *
             FROM "COMPANY"."T_REL_AHN_PERSON_ELTERN" 
             WHERE "FK_KON_PERSON_ELTERN" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/accounts/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                zus."FK_MAIN_KEY",
                zus."BUCHUNGSTAG",
                zus."BETRAG",
		zus."BUCHUNGSTEXT",
		zus."FK_KTO_KONTO_AUSZUG",
		zus."TBL",
                zus."FK_KTO_BANKKONTO",
                aus."FINAL_CNT",
                aus."FINAL_AMOUNT",
                "SEITE",
                zus."STATUS",
                pay."TRANSAKTIONSCODE",
                pay."RECHNUNGSNUMMER"
             FROM "COMPANY"."V_KTO_KONTEN_ZUS" zus
                LEFT JOIN "COMPANY"."T_KTO_KONTO_AUSZUG" aus on zus."FK_KTO_KONTO_AUSZUG" = aus."PK_KTO_KONTO_AUSZUG"
                LEFT JOIN "COMPANY"."T_KTO_PAYPAL" pay on pay."FK_MAIN_KEY" = zus."FK_MAIN_KEY"
             WHERE zus."FK_KTO_KONTO_AUSZUG" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get related documents by FK_INP_BELEGE_ALL (checks both FK_INP_BELEGE_ALL1 and FK_INP_BELEGE_ALL2)
app.get("/belege/belege/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid document ID" });
        }

        const result = await pool.query(
            ` SELECT relinp.* ,
          inp1."PK_INP_BELEGE_ALL",
          inp1."BELEGNUMMER"
          inp1."BEZEICHNUNG",
          inp1."BRUTTO_BETRAG",
          inp1."BELEGDATUM",
          inp1."RECHNUNGSDATUM",
          inp2."PK_INP_BELEGE_ALL" inp2_PK_INP_BELEGE_ALL,
          inp2."BELEGNUMMER" inp2_BELEGNUMMER,
          inp2."BEZEICHNUNG" inp2_BEZEICHNUNG,
          inp2."BRUTTO_BETRAG" inp2_BRUTTO_BETRAG,
          inp2."BELEGDATUM" inp2_BELEGDATUM,
          inp2."RECHNUNGSDATUM" inp2_RECHNUNGSDATUM
          
   
             FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_INP_BELEGE_ALL" relinp
               left join "COMPANY"."T_INP_BELEGE_ALL" inp1 on inp1."PK_INP_BELEGE_ALL" = relinp."FK_INP_BELEGE_ALL1" 
                left join "COMPANY"."T_INP_BELEGE_ALL" inp2 on inp2."PK_INP_BELEGE_ALL" = relinp."FK_INP_BELEGE_ALL2"
             WHERE "FK_INP_BELEGE_ALL1" = $1 OR "FK_INP_BELEGE_ALL2" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ 
                message: "No related documents found",
                details: `No records found where FK_INP_BELEGE_ALL1 or FK_INP_BELEGE_ALL2 equals ${id}`
            });
        }

        res.json({
            count: result.rows.length,
            documents: result.rows
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch related documents",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/accounts/1/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT "PK_REL_LEX_KTO_BEL",
                "FK_MAIN_KEY",
                "FK_INP_BELEGE_ALL",
                "FK_LEX_RELATION",
		"LINK_LEXOFFICE_BUCHUNG"
             FROM "COMPANY"."T_REL_LEX_KTO_BEL" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/accounts/2/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT rel."PK_REL_LEX_KTO_BEL",
                rel."FK_MAIN_KEY",
                rel."FK_INP_BELEGE_ALL",
                rel."FK_LEX_RELATION",
		rel."LINK_LEXOFFICE_BUCHUNG",
                rel."FLG_LEXOFFICE_BUCHUNG",
		rel."FLG_LEXOFFICE_MIT_BILD",
                zus."BUCHUNGSTEXT",
                zus."BETRAG",
                to_char(zus."BUCHUNGSTAG",'DD.MM.YYYY'),
                zus."FK_KTO_BANKKONTO",
                zus."FK_KON_OWNER1",
                zus."OWNER1",
                zus."BUCHT_JAHR",
                zus."TBL",
                '<a href=' || rel."LINK_LEXOFFICE_BUCHUNG" || ' target="_blank" 
   rel="noopener noreferrer"
   class="lexoffice-link">
   ' || rel."LINK_LEXOFFICE_BUCHUNG" || '
</a>' link
             FROM "COMPANY"."T_REL_LEX_KTO_BEL" rel
                 left Join "COMPANY"."V_KTO_KONTEN_ZUS" zus on rel."FK_MAIN_KEY" = zus."FK_MAIN_KEY"
             WHERE "FK_INP_BELEGE_ALL" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get accounts with flexible AND filtering on multiple columns
app.get("/accounts/single", async (req, res) => {
    try {
        // Extrahiere Query-Parameter
        const { pk_rel_lex_kto_bel, fk_main_key, fk_inp_belege_all, fk_lex_relation, link_lexoffice_buchung } = req.query;

        // Baue die SQL-Abfrage dynamisch auf
        let query = `SELECT "PK_REL_LEX_KTO_BEL",
                            "FK_MAIN_KEY",
                            "FK_INP_BELEGE_ALL",
                            "FK_LEX_RELATION",
                            "LINK_LEXOFFICE_BUCHUNG"
                     FROM "COMPANY"."T_REL_LEX_KTO_BEL"
                     WHERE 1=1 `; // Basis, um flexibel weitere Bedingungen hinzuzufügen

        const params = [];
        let paramIndex = 1;

        // Füge Bedingungen nur hinzu, wenn Parameter existieren
        if (pk_rel_lex_kto_bel !== undefined) {
            query += ` AND "PK_REL_LEX_KTO_BEL" = $${paramIndex++}`;
            params.push(pk_rel_lex_kto_bel);
        }
        if (fk_main_key !== undefined) {
            query += ` AND "FK_MAIN_KEY" = $${paramIndex++}`;
            params.push(fk_main_key);
        }
        if (fk_inp_belege_all !== undefined) {
            query += ` AND "FK_INP_BELEGE_ALL" = $${paramIndex++}`;
            params.push(fk_inp_belege_all);
        }
        if (fk_lex_relation !== undefined) {
            query += ` AND "FK_LEX_RELATION" = $${paramIndex++}`;
            params.push(fk_lex_relation);
        }
        if (link_lexoffice_buchung !== undefined) {
            query += ` AND "LINK_LEXOFFICE_BUCHUNG" = $${paramIndex++}`;
            params.push(link_lexoffice_buchung);
        }

        // Führe die Abfrage aus
        const result = await pool.query(query, params);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "No matching account found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});
// Get all accounts
app.get("/accounts/bild/2/", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
                        "PK_KTO_KONTO_AUSZUG",
			"IBAN"
                    FROM 
                        "COMPANY"."V_KTO_KONTO_AUSZUG_BILDER" 
		    group by  "PK_KTO_KONTO_AUSZUG",
				"IBAN"

                   ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/accounts/bild/1/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."V_KTO_KONTO_AUSZUG_BILDER" 
             WHERE "PK_KTO_KONTO_AUSZUG" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

const fs = require('fs');
const path = require('path');

app.get("/accounts/bild/1/1/:id", async (req, res) => {
    try {
        const { id } = req.params;
        const cacheDir = path.join(__dirname, 'temp_cache');
        
        // Validate ID parameter
        if (!id || !/^\d+$/.test(id)) {
            return res.status(400).json({ 
                error: "Invalid account ID",
                details: "ID must be a positive integer"
            });
        }

        // Create cache directory if it doesn't exist
        if (!fs.existsSync(cacheDir)) {
            fs.mkdirSync(cacheDir, { recursive: true });
        }

        const cachePath = path.join(cacheDir, `image_${id}.cache`);

        // Check cache first
        if (fs.existsSync(cachePath)) {
            const cachedData = JSON.parse(fs.readFileSync(cachePath));
            const { FILECONTENT, FILENAME, KLASSIFIKATION_1, KLASSIFIKATION_2, lastUpdated,DATUM_ZUORD_OK, FINAL_CNT_FK_INP_BELEGE_ALL, FINAL_CNT_FK_KON_PERSON  } = cachedData;
            
            // Verify cache is fresh (e.g., 1 hour cache)
            if (Date.now() - lastUpdated < 3600000) {
                const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
                
                // Prepare response data
                const response = {
                    file: {
                        content: Buffer.from(FILECONTENT),
                        filename: FILENAME,
                        contentType: isPDF ? 'application/pdf' : getContentType(FILENAME)
                        
                    },
                    metadata: {
                        klassifikation1: KLASSIFIKATION_1,
                        klassifikation2: KLASSIFIKATION_2,
                        datum_zuord_ok: DATUM_ZUORD_OK
                        
                    }
                };

                // Set headers and send response
                res.setHeader('Content-Type', 'application/json');
                return res.json(response);
            }
        }

        // Database query
        const result = await pool.query(
            `SELECT "FILECONTENT", "FILENAME", "KLASSIFIKATION_1", "KLASSIFIKATION_2",
               "DATUM_ZUORD_OK", "FINAL_CNT_FK_INP_BELEGE_ALL", "FINAL_CNT_FK_KON_PERSON"
             FROM "COMPANY"."T_BILD_BILDER" 
             WHERE "PK_BILD_BILDER" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ 
                error: "Image not found",
                details: `No image found with ID ${id}`
            });
        }

        const { FILECONTENT, FILENAME, KLASSIFIKATION_1, KLASSIFIKATION_2 } = result.rows[0];

        if (!FILECONTENT || !FILENAME) {
            return res.status(404).json({ 
                error: "Invalid image data",
                details: "File content or filename missing"
            });
        }

        // Update cache
        const cacheData = {
            FILECONTENT: Array.from(FILECONTENT),
            FILENAME,
            KLASSIFIKATION_1,
            KLASSIFIKATION_2,
            lastUpdated: Date.now()
        };
        fs.writeFileSync(cachePath, JSON.stringify(cacheData));

        // Prepare response data
        const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
        const response = {
            file: {
                content: FILECONTENT,
                filename: FILENAME,

                contentType: isPDF ? 'application/pdf' : getContentType(FILENAME)
            },
            metadata: {
                klassifikation1: KLASSIFIKATION_1,
                klassifikation2: KLASSIFIKATION_2,
                datum_zuord_ok: DATUM_ZUORD_OK,
                final_cnt_fk_inp_belege_all: FINAL_CNT_FK_INP_BELEGE_ALL,
                final_cnt_fk_kon_person: FINAL_CNT_FK_KON_PERSON
            }
        };

        // Set headers
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Cache-Control', 'public, max-age=3600'); // 1 hour cache

        // Send response
        res.json(response);

    } catch (error) {
        console.error("Image retrieval error:", error);
        
        // Specific error for database issues
        if (error.code === 'ECONNREFUSED') {
            return res.status(503).json({
                error: "Database unavailable",
                details: process.env.NODE_ENV === 'development' ? error.message : undefined
            });
        }

        res.status(500).json({ 
            error: "Image retrieval failed",
            details: process.env.NODE_ENV === 'development' ? error.message : undefined,
            requestId: req.id // If you have request ID tracking
        });
    }
});


app.get("/accounts/bild/1/1/1/:id", async (req, res) => {
    try {
        const { id } = req.params;
        const cacheDir = path.join(__dirname, 'temp_cache');
        
        // Validate ID parameter
        if (!id || !/^\d+$/.test(id)) {
            return res.status(400).json({ 
                error: "Invalid account ID",
                details: "ID must be a positive integer"
            });
        }

        // Create cache directory if it doesn't exist
        if (!fs.existsSync(cacheDir)) {
            fs.mkdirSync(cacheDir, { recursive: true });
        }

        const cachePath = path.join(cacheDir, `image_${id}.cache`);

        // Check cache first
        if (fs.existsSync(cachePath)) {
            const cachedData = JSON.parse(fs.readFileSync(cachePath));
            const { FILECONTENT, FILENAME, lastUpdated } = cachedData;
            
            // Verify cache is fresh (e.g., 1 hour cache)
            if (Date.now() - lastUpdated < 3600000) {
                const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
                res.setHeader('Content-Type', isPDF ? 'application/pdf' : getContentType(FILENAME));
                res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(FILENAME)}"`);
                return res.send(Buffer.from(FILECONTENT));
            }
        }

        // Database query
        const result = await pool.query(
            `SELECT "FILECONTENT", "FILENAME"
             FROM "COMPANY"."T_BILD_BILDER" 
             WHERE "PK_BILD_BILDER" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ 
                error: "Image not found",
                details: `No image found with ID ${id}`
            });
        }

        const { FILECONTENT, FILENAME} = result.rows[0];

        if (!FILECONTENT || !FILENAME) {
            return res.status(404).json({ 
                error: "Invalid image data",
                details: "File content or filename missing"
            });
        }

        // Update cache
        const cacheData = {
            FILECONTENT: Array.from(FILECONTENT),
            FILENAME,
            lastUpdated: Date.now()
        };
        fs.writeFileSync(cachePath, JSON.stringify(cacheData));

        // Set response headers
        const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
        res.setHeader('Content-Type', isPDF ? 'application/pdf' : getContentType(FILENAME));
        res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(FILENAME)}"`);
        res.setHeader('Cache-Control', 'public, max-age=3600'); // 1 hour cache


        // Send response
        res.send(FILECONTENT);

    } catch (error) {
        console.error("Image retrieval error:", error);
        
        // Specific error for database issues
        if (error.code === 'ECONNREFUSED') {
            return res.status(503).json({
                error: "Database unavailable",
                details: process.env.NODE_ENV === 'development' ? error.message : undefined
            });
        }

        res.status(500).json({ 
            error: "Image retrieval failed",
            details: process.env.NODE_ENV === 'development' ? error.message : undefined,
            requestId: req.id // If you have request ID tracking
        });
    }
});

// Helper function to determine content type from filename
function getContentType(filename) {
    const extension = filename.split('.').pop().toLowerCase();
    const typeMap = {
        'jpg': 'image/jpeg',
        'jpeg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif',
        'svg': 'image/svg+xml',
        'webp': 'image/webp'
    };
    return typeMap[extension] || 'application/octet-stream';
}


// Get all accounts
app.get("/ordner_pages", async (req, res) => {
    try {
        const result = await pool.query(
            `SELECT 
               *

                    FROM 
                        "COMPANY"."V_ABL_ORDNER_PAGE" 
                    ;`
        );
        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get all accounts for a specific year
app.get("/ordner_pages/:jahr", async (req, res) => {
    try {
        const { jahr } = req.params;

        // Validate year parameter
        if (!jahr || isNaN(jahr) || jahr.length !== 4) {
            return res.status(400).json({ 
                error: "Invalid year parameter",
                message: "Please provide a valid 4-digit year"
            });
        }

        const result = await pool.query(
            `SELECT * 
             FROM "COMPANY"."V_ABL_ORDNER_PAGE" 
             WHERE "JAHR" = $1`,
            [jahr]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ 
                message: "No records found for the specified year",
                year: jahr
            });
        }

        res.json({
            count: result.rows.length,
            data: result.rows
        });

    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch accounts",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/belege/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_INP_BELEGE_ALL" 
             WHERE "FK_ABL_ORDNER_PAGE" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/beleg/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_INP_BELEGE_ALL" 
             WHERE "PK_INP_BELEGE_ALL" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/belege_pos/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_INP_BELEGE_POS_ALL" 
             WHERE "FK_INP_BELEGE_ALL" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/beleg/bild/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                inp.*, bild."PK_BILD_BILDER",bild."FILECONTENT", bild."FILENAME", bild."KLASSIFIKATION_1", bild."DATUM_ZUORD_OK", bild."FINAL_CNT_FK_INP_BELEGE_ALL", bild."FINAL_CNT_FK_KON_PERSON", bild."FINAL_CNT_FK_INV_INVENTARE"
             FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" inp
                 left join "COMPANY"."T_BILD_BILDER" bild on inp."FK_BILD_BILDER" = bild."PK_BILD_BILDER"
             WHERE "FK_INP_BELEGE_ALL" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/beleg/bild1/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                relinp.*, bild."PK_BILD_BILDER",bild."FILECONTENT", bild."FILENAME", bild."KLASSIFIKATION_1", inp."PK_INP_BELEGE_ALL", inp."BEZEICHNUNG", inp."BELEGDATUM", inp."BRUTTO_BETRAG", bild."DATUM_ZUORD_OK", bild."FINAL_CNT_FK_INP_BELEGE_ALL", bild."FINAL_CNT_FK_KON_PERSON", inp."DATUM_ALL_OK"
             FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" relinp
                 left join "COMPANY"."T_BILD_BILDER" bild on relinp."FK_BILD_BILDER" = bild."PK_BILD_BILDER"
                 left join "COMPANY"."T_INP_BELEGE_ALL" inp on inp."PK_INP_BELEGE_ALL" = relinp."FK_INP_BELEGE_ALL"
             WHERE "FK_BILD_BILDER" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get beleg/bild by either FK_INP_BELEGE_ALL or FK_BILD_BILDER
app.get("/beleg/bild1", async (req, res) => {
    try {
        const { fk_inp_belege_all, fk_bild_bilder } = req.query;
        
        // Validate at least one parameter is provided
        if (!fk_inp_belege_all && !fk_bild_bilder) {
            return res.status(400).json({ 
                error: "Must provide either FK_INP_BELEGE_ALL or FK_BILD_BILDER parameter",
                example_usage: [
                    "/beleg/bild?fk_inp_belege_all=123",
                    "/beleg/bild?fk_bild_bilder=456",
                    "/beleg/bild?fk_inp_belege_all=123&fk_bild_bilder=456"
                ]
            });
        }

        // Validate parameters are numbers if provided
        if (fk_inp_belege_all && isNaN(fk_inp_belege_all)) {
            return res.status(400).json({ 
                error: "Invalid FK_INP_BELEGE_ALL value",
                received: fk_inp_belege_all
            });
        }
        if (fk_bild_bilder && isNaN(fk_bild_bilder)) {
            return res.status(400).json({ 
                error: "Invalid FK_BILD_BILDER value",
                received: fk_bild_bilder
            });
        }

        // Build query dynamically based on provided parameters
        let query = `
            SELECT 
                inp.*, 
                bild."PK_BILD_BILDER",
                bild."FILECONTENT", 
                bild."FILENAME", 
                bild."KLASSIFIKATION_1"
            FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" inp
            LEFT JOIN "COMPANY"."T_BILD_BILDER" bild 
                ON inp."FK_BILD_BILDER" = bild."PK_BILD_BILDER"
            WHERE 1=1 `;

        const params = [];
        
        if (fk_inp_belege_all && fk_bild_bilder) {
            query += ` AND ("FK_INP_BELEGE_ALL" = $1 OR "FK_BILD_BILDER" = $2)`;
            params.push(fk_inp_belege_all, fk_bild_bilder);
        } else if (fk_inp_belege_all) {
            query += `"AND FK_INP_BELEGE_ALL" = $1`;
            params.push(fk_inp_belege_all);
        } else {
            query += `"AND FK_BILD_BILDER" = $1`;
            params.push(fk_bild_bilder);
        }

        const result = await pool.query(query, params);

        if (result.rows.length === 0) {
            return res.status(404).json({ 
                error: "No records found",
                parameters_used: {
                    fk_inp_belege_all,
                    fk_bild_bilder
                }
            });
        }

        res.json({
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch records",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack,
                query: error.query
            } : undefined
        });
    }
});


app.get("/related-documents/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid document ID" });
        }

        // Convert to number to prevent SQL injection
        const documentId = Number(id);

        const queryText = `
            SELECT 
                relinp.*,
                inp1."PK_INP_BELEGE_ALL" AS inp1_pk,
                inp1."BELEGNUMMER" AS inp1_belegnummer,
                inp1."BEZEICHNUNG" AS inp1_bezeichnung,
                inp1."BELEGDATUM" AS inp1_belegdatum,
                inp1."BRUTTO_BETRAG" AS inp1_bruttobetrag,
                inp2."PK_INP_BELEGE_ALL" AS inp2_pk,
                inp2."BELEGNUMMER" AS inp2_belegnummer,
                inp2."BEZEICHNUNG" AS inp2_bezeichnung,
                inp2."BELEGDATUM" AS inp2_belegdatum,
                inp2."BRUTTO_BETRAG" AS inp2_bruttobetrag
            FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_INP_BELEGE_ALL" relinp
            LEFT JOIN "COMPANY"."T_INP_BELEGE_ALL" inp1 
                ON inp1."PK_INP_BELEGE_ALL" = relinp."FK_INP_BELEGE_ALL1"
            LEFT JOIN "COMPANY"."T_INP_BELEGE_ALL" inp2 
                ON inp2."PK_INP_BELEGE_ALL" = relinp."FK_INP_BELEGE_ALL2"
            WHERE relinp."FK_INP_BELEGE_ALL1" = $1 
               OR relinp."FK_INP_BELEGE_ALL2" = $1`;

        const result = await pool.query(queryText, [documentId]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "No related documents found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch related documents",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});

app.get("/pos-related-documents/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid document ID" });
        }

        // Convert to number to prevent SQL injection
        const documentId = id;

        const queryText = `
            SELECT 
                pos."FK_INP_BELEGE_ALL",
                pos."POSITION",
                pos."BELEGNUMMER",
                pos."BELEGDATUM",
                pos."BEZEICHNUNG",
                pos."NETTO_BETRAG",
                pos."BRUTTO_BETRAG",
                pos."FK_INP_BELEGE_ALL_RELATED",
                inp1."PK_INP_BELEGE_ALL" AS inp1_pk,
                inp1."BELEGNUMMER" AS inp1_belegnummer,
                inp1."BEZEICHNUNG" AS inp1_bezeichnung,
                inp1."BELEGDATUM" AS inp1_belegdatum,
                inp1."BRUTTO_BETRAG" AS inp1_bruttobetrag,
                inp2."PK_INP_BELEGE_ALL" AS inp2_pk,
                inp2."BELEGNUMMER" AS inp2_belegnummer,
                inp2."BEZEICHNUNG" AS inp2_bezeichnung,
                inp2."BELEGDATUM" AS inp2_belegdatum,
                inp2."BRUTTO_BETRAG" AS inp2_bruttobetrag
            FROM "COMPANY"."T_INP_BELEGE_POS_ALL" pos
            LEFT JOIN "COMPANY"."T_INP_BELEGE_ALL" inp1 
                ON inp1."PK_INP_BELEGE_ALL"::text = pos."FK_INP_BELEGE_ALL"
            LEFT JOIN "COMPANY"."T_INP_BELEGE_ALL" inp2 
                ON inp2."PK_INP_BELEGE_ALL" = pos."FK_INP_BELEGE_ALL_RELATED"
            WHERE pos."FK_INP_BELEGE_ALL"::text =$1::text
               `;

        const result = await pool.query(queryText, [documentId]);

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "No related documents found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch related documents",
            details: process.env.NODE_ENV === "development" ? {
                message: error.message,
                stack: error.stack
            } : undefined
        });
    }
});


// Get single account by ID with proper field mapping
app.get("/giro/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_KTO_GIROKONTO" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/kredit/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_KTO_KREDITKARTE" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/paypal/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_KTO_PAYPAL" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/paypal_gr/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_KTO_PAYPAL_GUTHABEN_RELEVANT" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});

// Get single account by ID with proper field mapping
app.get("/paypal_abgeschl/:id", async (req, res) => {
    try {
        const { id } = req.params;
        
        // Validate ID parameter
        if (!id || isNaN(id)) {
            return res.status(400).json({ error: "Invalid account ID" });
        }

        const result = await pool.query(
            `SELECT 
                *
             FROM "COMPANY"."T_KTO_PAYPAL_ABGESCHLOSSENE_ZAHLUNGEN" 
             WHERE "FK_MAIN_KEY" = $1`,
            [id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ error: "Account not found" });
        }

        res.json(result.rows);
    } catch (error) {
        console.error("Database error:", error.message);
        res.status(500).json({ 
            error: "Failed to fetch account",
            details: process.env.NODE_ENV === "development" ? error.message : undefined
        });
    }
});



// Start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});