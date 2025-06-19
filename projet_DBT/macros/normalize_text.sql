{% macro normalize_text(column_name, options) %}
    {# Sécurité si options est null ou non fourni #}
    {% set options = options or {} %}
    {% set remove_stopwords = options.get('remove_stopwords', true) %}
    {% set min_length = options.get('min_length', 2) %}
    {% set keep_numbers = options.get('keep_numbers', true) %}
    {% set languages = options.get('languages', ['fr', 'ar', 'en']) %}

    CASE 
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN TRIM({{ column_name }}) = '' THEN NULL
        WHEN LENGTH(TRIM({{ column_name }})) < {{ min_length }} THEN NULL
        ELSE
            TRIM(
                REGEXP_REPLACE(
                    {% if remove_stopwords %}
                        {% if 'en' in languages %}REGEXP_REPLACE({% endif %}
                        {% if 'ar' in languages %}REGEXP_REPLACE({% endif %}
                        {% if 'fr' in languages %}REGEXP_REPLACE({% endif %}
                            REGEXP_REPLACE(
                                LOWER(TRIM({{ column_name }})),
                                {% if keep_numbers %}
                                '[^a-zA-Z0-9àâäçéèêëïîôùûüÿñæœ\s]'
                                {% else %}
                                '[^a-zA-Zàâäçéèêëïîôùûüÿñæœ\s]'
                                {% endif %},
                                ' ',
                                'g'
                            )
                        {% if 'fr' in languages %}
                            ,'\y(le|l|m|moi|en|c|a|la|j|d|est|n|ai|les|un|une|des|de|du|et|ou|à|au|aux|ce|cette|ces|pour|par|avec|sans|sur|sous|dans|que|qui|quoi|comment|très|plus|moins|bien|mal|tout|tous|toute|toutes|avoir|être|faire|aller|venir|voir|savoir|pouvoir|vouloir|devoir|son|sa|ses|mon|ma|mes|ton|ta|tes|notre|nos|votre|vos|leur|leurs|je|tu|il|elle|nous|vous|ils|elles|me|te|se|lui|ne|pas|non|oui|si|mais|car|donc|or|ni|comme|quand|où|dont|depuis|pendant|avant|après|chez|vers|contre|entre|parmi|selon|sauf|alors|ainsi|aussi|cependant|pourtant|lorsque|puisque|afin|ici|là|maintenant|hier|demain|toujours|jamais|souvent|parfois|déjà|encore|bientôt|beaucoup|peu|assez|trop|vraiment|peut|être|même|surtout|plutôt|quelque|chose|quelques|plusieurs|certains|autres|chaque|chacun|autre|sur|sous|dans|par|pour|avec|sans|chez|vers|dès|jusque|jusqu|malgré|grâce|concernant|durant|moyennant|via|excepté|hormis|outre|près|loin|dedans|dehors|dessus|dessous|devant|derrière|côté|face|autour|environ|parmi|among|amid|through|across|along|beside|beyond|beneath|above|below|inside|outside|within|without|throughout|around|toward|towards|upon|unto|into|onto|off|out|over|under|up|down|back|forth|away|ahead|behind|beside|past|beyond|amid|among|between|through|across|along|around|about|concerning|regarding|respecting|touching|including|excluding|except|besides|plus|minus|times|versus|against|despite|notwithstanding|considering|given|granted|provided|supposing|assuming|barring|failing|lacking|wanting|pending|during|while|whilst|whereas|whereby|wherein|whereon|whereof|wherefor|wherefore|whereat|whereto|whereunto|wherefrom|whereabouts|whereafter|whereupon|wherewithal|whereas|whereby|wherein|whereon|whereof|wherefor|wherefore|whereat|whereto|whereunto|wherefrom|whereabouts|whereafter|whereupon|wherewithal|tandis|pendant|durant|lors|dès|depuis|jusqu|avant|après|auparavant|désormais|dorénavant|actuellement|présentement|immédiatement|aussitôt|sitôt|bientôt|tantôt|parfois|quelquefois|souvent|fréquemment|rarement|jamais|toujours|constamment|continuellement|perpétuellement|éternellement|temporairement|provisoirement|momentanément|instantanément|subitement|soudain|soudainement|brusquement|brutalement|violemment|doucement|lentement|rapidement|vite|vitesse|prestement|hâtivement|précipitamment|immédiatement|directement|indirectement|clairement|nettement|distinctement|précisément|exactement|approximativement|environ|presque|quasi|pratiquement|virtuellement|réellement|véritablement|vraiment|certainement|sûrement|probablement|possiblement|éventuellement|peut-être|sans|doute|assurément|effectivement|en|effet|d|ailleurs|par|ailleurs|néanmoins|toutefois|cependant|pourtant|malgré|nonobstant|quoique|bien|que|encore|que|même|si|quand|même|lors|même|que|alors|que|tandis|que|pendant|que|durant|que|depuis|que|jusqu|ce|que|avant|que|après|que|dès|que|sitôt|que|aussitôt|que|maintenant|que|actuellement|que|présentement|que|immédiatement|que|directement|que|indirectement|que|clairement|que|nettement|que|distinctement|que|précisément|que|exactement|que|approximativement|que|environ|que|presque|que|quasi|que|pratiquement|que|virtuellement|que|réellement|que|véritablement|que|vraiment|que|certainement|que|sûrement|que|probablement|que|possiblement|que|éventuellement|que|peut-être|que|sans|que|doute|que|assurément|que|effectivement|que|en|que|effet|que|d|que|ailleurs|que|par|que|ailleurs|que|néanmoins|que|toutefois|que|cependant|que|pourtant|que|malgré|que|nonobstant|que|quoique|que|bien|que|encore|que|même|que|quand|que|même|que|lors|que|même|que|alors|que|tandis|que|pendant|que|durant|que|depuis|que|jusqu|que|ce|que|avant|que|après|que|dès|que|sitôt|que|aussitôt|que|maintenant|que|actuellement|que|présentement|que|immédiatement|que|directement|que|indirectement|que|clairement|que|nettement|que|distinctement|que|précisément|que|exactement|que|approximativement|que|environ|que|presque|que|quasi|que|pratiquement|que|virtuellement|que|réellement|que|véritablement|que|vraiment|que|certainement|que|sûrement|que|probablement|que|possiblement|que|éventuellement|que|peut-être|que|sans|que|doute|que|assurément|que|effectivement|que|oui|non|certes|bien|sûr|évidemment|naturellement|évidemment|forcément|nécessairement|obligatoirement|impérativement|absolument|totalement|complètement|entièrement|parfaitement|exactement|précisément|rigoureusement|strictement|uniquement|seulement|simplement|purement|exclusivement|particulièrement|spécialement|notamment|surtout|principalement|essentiellement|fondamentalement|globalement|généralement|habituellement|ordinairement|communément|couramment|fréquemment|régulièrement|normalement|traditionnellement|classiquement|typiquement|caractéristiquement|spécifiquement|distinctement|différemment|autrement|sinon|sans|quoi|faute|de|quoi|à|défaut|de|quoi|hormis|cela|excepté|cela|sauf|cela|mis|à|part|cela|outre|cela|en|plus|de|cela|par|surcroît|de|plus|en|outre|qui|plus|est|par|ailleurs|d|autre|part|d|un|autre|côté|en|revanche|au|contraire|à|l|inverse|à|l|opposé|contrairement|à|l|encontre|de|contre|toute|attente|malgré|tout|en|dépit|de|tout|nonobstant|tout|quoi|qu|il|en|soit)\y',
                            ' ',
                            'gi'
                        )
                        {% endif %}
                        {% if 'ar' in languages %}
                            ,'\y(في|من|إلى|على|عن|مع|هذا|هذه|ذلك|تلك|التي|الذي|كان|كانت|يكون|تكون|أن|أو|أي|إذا|كل|بعض|جميع|عند|عندما|حتى|لكن|كيف|متى|أين|ماذا|لماذا|هل|نعم|لا|ليس|ليست|غير|بل|حيث|بعد|قبل|أثناء|خلال|فوق|تحت|أمام|خلف|بين|ضد|نحو|حول|دون|بدون|ضمن|خارج|داخل|فقط|أيضا|كذلك|إذن|لذلك|هنا|هناك|الآن|أمس|غدا|اليوم|دائما|أبدا|أحيانا|ربما|قد|لقد|لم|لن|ما|ال|كم|كما|جدا|سوف)\y',
                            ' ',
                            'g'
                        )
                        {% endif %}
                        {% if 'en' in languages %}
                            ,'\y(a|about|above|after|again|against|all|am|an|and|any|are|as|at|be|because|been|before|being|below|between|both|but|by|can|could|did|do|does|doing|down|during|each|few|for|from|further|had|has|have|having|he|her|here|hers|herself|him|himself|his|how|if|in|into|is|it|its|itself|let|me|more|most|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|same|she|should|so|some|such|than|that|the|their|theirs|them|themselves|then|there|these|they|this|those|through|to|too|under|until|up|very|was|we|were|what|when|where|which|while|who|whom|why|with|would|you|your|yours|yourself|yourselves|also|although|always|among|another|around|become|comes|get|give|go|goes|going|gone|got|keep|know|last|make|may|might|much|must|need|never|new|now|old|one|said|say|see|since|still|take|tell|think|time|today|tomorrow|use|want|way|well|went|will|without|work|year|yet)\y',
                            ' ',
                            'gi'
                        )
                        {% endif %}
                    {% else %}
                        REGEXP_REPLACE(
                            LOWER(TRIM({{ column_name }})),
                            {% if keep_numbers %}
                            '[^a-zA-Z0-9àâäçéèêëïîôùûüÿñæœ\s]'
                            {% else %}
                            '[^a-zA-Zàâäçéèêëïîôùûüÿñæœ\s]'
                            {% endif %},
                            ' ',
                            'g'
                        )
                    {% endif %},
                    '\s+',
                    ' ',
                    'g'
                )
            )
    END
{% endmacro %}


{% macro clean_text_basic(column_name, keep_numbers=false) %}
    CASE 
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN TRIM({{ column_name }}) = '' THEN NULL
        ELSE
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        LOWER(TRIM({{ column_name }})),
                        {% if keep_numbers %}
                        '[^a-zA-Z0-9àâäçéèêëïîôùûüÿñæœ\s]'
                        {% else %}
                        '[^a-zA-Zàâäçéèêëïîôùûüÿñæœ\s]'
                        {% endif %}, 
                        ' ', 
                        'g'
                    ),
                    '\s+',
                    ' ',
                    'g'
                )
            )
    END
{% endmacro %}
-- Version alternative plus explicite pour supprimer les chiffres
{% macro clean_text_no_digits(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN TRIM({{ column_name }}) = '' THEN NULL
        ELSE
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            LOWER(TRIM({{ column_name }})),
                            '[0-9]',  -- Supprime d'abord tous les chiffres
                            '',
                            'g'
                        ),
                        '[^a-zA-Zàâäçéèêëïîôùûüÿñæœ\s]',  -- Puis supprime les caractères spéciaux
                        ' ',
                        'g'
                    ),
                    '\s+',  -- Enfin, normalise les espaces
                    ' ',
                    'g'
                )
            )
    END
{% endmacro %}

-- Macro helper pour validation de texte nettoyé
{% macro is_valid_text(text_column, min_length=3, allow_numbers=true) %}
    {{ text_column }} IS NOT NULL 
    AND TRIM({{ text_column }}) != ''
    AND LENGTH(TRIM({{ text_column }})) >= {{ min_length }}
    {% if not allow_numbers %}
    AND NOT (TRIM({{ text_column }}) ~ '^[0-9\s]*$')
    {% endif %}
{% endmacro %}

