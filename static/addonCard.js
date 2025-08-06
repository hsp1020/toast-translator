/*
const addonBlackList = [
    "org.stremio.mammamia",                 // Mamma Mia
]
*/

const compatibilityList = [
    "com.linvo.cinemeta",               // Cinemeta
    "community.anime.kitsu",            // Kitsu 
    "org.stremio.animecatalogs",        // Anime Catalog
    "tmdb-addon",                       // TMDB Addon
    "org.stremio.aiolists",
    "com.aiostreams.viren070",
    "com.aiostreams.viren070.ed7de9be-ad4",
    "com.aiostreams.viren070.",
    "marcojoao.ml.cyberflix.catalog",   // Cyberflix Catalogs
    "pw.ers.netflix-catalog",           // Streaming Catalogs
    "org.trakt.",                       // Trakt Stremio Official
    "community.trakt-tv",               // Trakt TV
    "org.stremio.pubdomainmovies",      // Public Domains
    "org.imdbcatalogs",                 // IMDB Catalogs
    "org.imdbcatalogs.rpdb",            // IMDB Catalogs (with ratings)
    "pw.ers.rottentomatoes",            // Rotten Tomatoes Catalogs
    "com.mdblist.",                     // MDBLists Catalogs
    "com.sagetendo.mal-stremio-addon",  // MAL Addon
    "dev.filmwhisper."                  // AI Film Whisper
]


async function loadAddon(url, showError=false, type="default") {
    if (!url) {
        alert("Invalid URL.");
        return;
    }

    try {
        const response = await fetch(url);
        if (response.ok) {
            const manifest = await response.json();
            const serverUrl = window.location.origin;
            if (compatibilityList.some(id => manifest.id.startsWith(id))) {
                if ("translated" in manifest && !url.includes(serverUrl)) {
                    return;
                }
                createAddonCard(manifest, url, type);
            } else {
                if (showError) {
                    alert("Addon non compatibile.");
                }
            }
        } else {
            if (showError){
                alert(`Error: ${response.status}`);
            }
        }

    } catch (error) {
        console.log(error);
    }
}

function createAddonCard(manifest, url, type="default") {
    const container = document.getElementById("addons-container");

    const addonCard = document.createElement("div");
    addonCard.className = "addon-info";

    addonCard.appendChild(createAddonHeader(manifest));
    addonCard.appendChild(createAddonDescription(manifest));
    addonCard.appendChild(createAddonVersion(manifest));
    addonCard.appendChild(createSkipPosterOption(manifest));
    addonCard.appendChild(createToastRatingsOption(manifest));

    const actionsDiv = document.createElement("div");
    if (type == "default") {
        actionsDiv.className = "addon-actions";
        const installBtn = createInstallButton(manifest, url);
        actionsDiv.appendChild(installBtn);
    } 
    else if (type == "generator") {
        actionsDiv.className = "addon-actions";
        const generateBtn = createGenerateButton(manifest, url);
        const copyBtn = createCopyButton(manifest, url);
        actionsDiv.appendChild(generateBtn);
        actionsDiv.appendChild(copyBtn);
        addonCard.appendChild(createLinkTextBox("", manifest));
    }
    

    addonCard.appendChild(actionsDiv);
    container.appendChild(addonCard);
}

function createAddonHeader(manifest) {
    const addonHeader = document.createElement("div");
    addonHeader.className = "addon-header";

    const logo = document.createElement("img");
    logo.className = "addon-logo";
    logo.src = manifest.logo || "static/img/addon_logo.png";
    logo.alt = "Logo dell'addon";
    addonHeader.appendChild(logo);

    const title = document.createElement("h3");
    title.innerText = manifest.name || "N/A";
    addonHeader.appendChild(title);

    return addonHeader;
}

function createAddonDescription(manifest) {
    const description = document.createElement("p");
    description.innerHTML = `<strong>Descrizione:</strong> ${manifest.description || "N/A"}`;
    return description;
}

function createAddonVersion(manifest) {
    const version = document.createElement("p");
    version.innerHTML = `<strong>Versione:</strong> ${manifest.version || "N/A"}`;
    return version;
}

function createSkipPosterOption(manifest) {
    const skipPosterDiv = document.createElement("div");
    skipPosterDiv.className = "skip-poster";

    const skipPosterCheckbox = document.createElement("input");
    skipPosterCheckbox.type = "checkbox";
    skipPosterCheckbox.id = `skipPoster-${manifest.name}`;
    skipPosterDiv.appendChild(skipPosterCheckbox);

    const skipPosterLabel = document.createElement("label");
    skipPosterLabel.htmlFor = `skipPoster-${manifest.name}`;
    skipPosterLabel.innerText = "Skip Poster";
    skipPosterDiv.appendChild(skipPosterLabel);

    return skipPosterDiv;
}

function createToastRatingsOption(manifest) {
    const toastRatingsDiv = document.createElement("div");
    toastRatingsDiv.className = "toast-ratings";

    const toastRatingsCheckbox = document.createElement("input");
    toastRatingsCheckbox.type = "checkbox";
    toastRatingsCheckbox.id = `toastRatings-${manifest.name}`;
    toastRatingsDiv.appendChild(toastRatingsCheckbox);

    const toastRatingsLabel = document.createElement("label");
    toastRatingsLabel.htmlFor = `toastRagings-${manifest.name}`;
    toastRatingsLabel.innerText = "Toast Ratings";
    toastRatingsDiv.appendChild(toastRatingsLabel);

    return toastRatingsDiv;
}

function createInstallButton(manifest, url) {
    const installBtn = document.createElement("button");
    installBtn.className = "install-btn";
    installBtn.innerText = "Seleziona";
    installBtn.state = "active";
    installBtn.onclick = () => toggleAddonSelection(installBtn, manifest, url);
    return installBtn;
}

function createGenerateButton(manifest, url) {
    const generateBtn = document.createElement("button");
    generateBtn.className = "generate-btn";
    generateBtn.innerText = "Genera link";
    generateBtn.onclick = () => generateLinkByCard(manifest, url, generateTranslatorLink);
    return generateBtn;
}

function createGenerateButton(manifest, url) {
    const generateBtn = document.createElement("button");
    generateBtn.className = "generate-btn";
    generateBtn.innerText = "Genera link";
    generateBtn.onclick = () => generateLinkByCard(manifest, url, generateTranslatorLink);
    return generateBtn;
}

function createCopyButton(manifest, url) {
    const generateBtn = document.createElement("button");
    generateBtn.className = "copy-btn";
    generateBtn.innerText = "Copia link";
    generateBtn.onclick = () => copyLinkCard(manifest);
    return generateBtn;
}

function createLinkTextBox(link, manifest) {
    const textArea = document.createElement("textarea");
    textArea.className = "read-only-textarea";
    textArea.id = `linkBox-${manifest.name}`;
    textArea.readOnly = true; 
    textArea.value = link;
    return textArea;
}


function toggleAddonSelection(installBtn, manifest, url) {
    const spCheckbox = document.getElementById(`skipPoster-${manifest.name}`);
    const trCheckbox = document.getElementById(`toastRatings-${manifest.name}`);
    if (installBtn.state === "active") {
        installBtn.state = "not_active";
        installBtn.innerText = "Rimuovi";
        installBtn.style.backgroundColor = "#ff4b4b";
        
        const skipQuery = spCheckbox.checked ? 1 : 0;
        const rateQuery = trCheckbox.checked ? 1 : 0;
        spCheckbox.disabled = true;
        trCheckbox.disabled = true;
        manifest.transportUrl = url;
        manifest.skipPoster = skipQuery;
        manifest.toastRatings = rateQuery;
        transteArray.push(manifest);
    } else {
        spCheckbox.disabled = false;
        trCheckbox.disabled = false;
        installBtn.state = "active";
        installBtn.innerText = "Seleziona";
        installBtn.style.backgroundColor = "#2ecc71";
        
        //Remove from translations selections
        transteArray = transteArray.filter(item => item !== manifest);
    }
}

async function copyLinkCard(manifest) {
    const linkBox = document.getElementById(`linkBox-${manifest.name}`);
    await navigator.clipboard.writeText(linkBox.value);
    alert('Link copiato!');
}

function generateLinkByCard(manifest, url, linkGeneratorFunc) {
    const spCheckbox = document.getElementById(`skipPoster-${manifest.name}`);
    const trCheckbox = document.getElementById(`toastRatings-${manifest.name}`);
    const linkBox = document.getElementById(`linkBox-${manifest.name}`)
    const skipQuery = spCheckbox.checked ? 1 : 0;
    const rateQuery = trCheckbox.checked ? 1 : 0;
    const link = linkGeneratorFunc(url, skipQuery, rateQuery);
    
    linkBox.value = link;
    linkBox.style.opacity = 100;
    linkBox.style.height = "auto";
    linkBox.style.height = (linkBox.scrollHeight) + "px";
}
