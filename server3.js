require('dotenv').config();
const express = require("express");
const bodyParser = require('body-parser');
const cors = require("cors");
const pool = require("./db");
const fs = require('fs');
const path = require('path');

const app = express();
const PORT =  5001;

// Middleware
app.use(cors());
// Configure bodyParser with appropriate limits
app.use(bodyParser.json({ limit: '1000mb' }));
app.use(bodyParser.urlencoded({ limit: '1000mb', extended: true }));
app.use(bodyParser.raw({ limit: '1000mb' }));

// Enhanced error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({ error: "Internal server error" });
});

app.use((req, res, next)=>{
    console.log(req.method, req.url, req.body);
    next();
})

app.get('/', (req, res) => {
  res.send('API is running!');
});

// Get all accounts
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
                	"FINAL_AMOUNT"

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


// Remove or comment out the default express.json() and express.urlencoded()
// app.use(express.json());
// app.use(express.urlencoded({ extended: true }));

// Handle file upload with binary storage
app.post("/person/bild/ins", async (req, res) => {
    try {
        const { category, description, title } = req.body;
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
/*
        // Validate base64 data
        if (!/^data:image\/(png|jpeg|jpg|gif|xml|tsv|csv|xlsx|pdf);base64,/.test(image)) {
            return res.status(400).json({ 
                error: "Invalid image format. Must be base64 encoded image" 
            });
        }
*/

        // Extract pure base64 data
        const base64Data = image.replace(/^data:image\/\w+;base64,/, '');
        const binaryData = Buffer.from(base64Data, 'base64');
        
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
             ("FILECONTENT", "FILENAME", "KLASSIFIKATION_1", "KLASSIFIKATION_2") 
             VALUES ($1::bytea, $2, $3,'Landscape') 
             RETURNING "PK_BILD_BILDER"`,
            [binaryData, filename, description || '']
        );

        res.status(201).json({ 
            success: true,
            message: "Image added successfully", 
            data: {
                id: result.rows[0].PK_BILD_BILDER,
                filename: result.rows[0].FILENAME,
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
                "FK_MAIN_KEY",
                "BUCHUNGSTAG",
                "BETRAG",
		"BUCHUNGSTEXT",
		zus."FK_KTO_KONTO_AUSZUG",
		"TBL",
                zus."FK_KTO_BANKKONTO",
                "FINAL_CNT",
                "FINAL_AMOUNT"
             FROM "COMPANY"."V_KTO_KONTEN_ZUS" zus
                LEFT JOIN "COMPANY"."T_KTO_KONTO_AUSZUG" aus on zus."FK_KTO_KONTO_AUSZUG" = aus."PK_KTO_KONTO_AUSZUG"
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
            try {
                const cachedData = JSON.parse(fs.readFileSync(cachePath));
                const { FILECONTENT, FILENAME, lastUpdated } = cachedData;
                
                // Verify cache is fresh (e.g., 1 hour cache)
                if (Date.now() - lastUpdated < 3600000) {
                    const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
                    res.setHeader('Content-Type', isPDF ? 'application/pdf' : getContentType(FILENAME));
                    res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(FILENAME)}"`);
                    return res.send(Buffer.from(FILECONTENT));
                }
            } catch (cacheError) {
                console.error("Cache read error:", cacheError);
                // Continue to database query if cache read fails
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

        const { FILECONTENT, FILENAME } = result.rows[0];

        if (!FILECONTENT || !FILENAME) {
            return res.status(404).json({ 
                error: "Invalid image data",
                details: "File content or filename missing"
            });
        }

        // Convert FILECONTENT to Buffer if it's not already
        const fileBuffer = Buffer.isBuffer(FILECONTENT) ? FILECONTENT : Buffer.from(FILECONTENT);

        // Update cache
        try {
            const cacheData = {
                FILECONTENT: Array.from(fileBuffer),
                FILENAME,
                lastUpdated: Date.now()
            };
            fs.writeFileSync(cachePath, JSON.stringify(cacheData));
        } catch (cacheError) {
            console.error("Cache write error:", cacheError);
            // Continue even if cache write fails
        }

        // Set response headers
        const isPDF = FILENAME.toLowerCase().endsWith('.pdf');
        res.setHeader('Content-Type', isPDF ? 'application/pdf' : getContentType(FILENAME));
        res.setHeader('Content-Disposition', `inline; filename="${encodeURIComponent(FILENAME)}"`);
        res.setHeader('Cache-Control', 'public, max-age=3600'); // 1 hour cache

        // Send response
        res.send(fileBuffer);

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
            details: process.env.NODE_ENV === 'development' ? error.message : undefined
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
                *
             FROM "COMPANY"."T_REL_INP_INP_BELEGE_ALL_BILD_BILDER" 
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



// Start server
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});