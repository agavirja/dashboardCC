import streamlit as st
def style(maxwidth=None):
    letra_options = '1vw'
    letra_titulo  = '1vw'
    
    if isinstance(maxwidth,(float,int)):
        if maxwidth < 1280:
            letra_options = '10px'
            letra_titulo  = '10px'
        elif 1280 <= maxwidth < 1600:
            letra_options = '12px'
            letra_titulo  = '12px'
        elif 1600 <= maxwidth < 1920:
            letra_options = '14px'
            letra_titulo  = '14px'
        else:
            letra_options = '16px'
            letra_titulo  = '16px'

    st.markdown(
        f"""
        <style>
    
        .stApp {{
            background-color: #fff;        
            opacity: 1;
            background-size: cover;
        }}
        
        div[data-testid="collapsedControl"] {{
            color: #000;
            }}
        
        div[data-testid="collapsedControl"] svg {{
            background-image: url('https://iconsapp.nyc3.digitaloceanspaces.com/house-black.png');
            background-size: cover;
            fill: transparent;
            width: 20px;
            height: 20px;
        }}
        
        div[data-testid="collapsedControl"] button {{
            background-color: transparent;
            border: none;
            cursor: pointer;
            padding: 0;
        }}

        div[data-testid="stToolbar"] {{
            visibility: hidden; 
            height: 0%; 
            position: fixed;
            }}
        div[data-testid="stDecoration"] {{
            visibility: hidden; 
            height: 0%; 
            position: fixed;
            }}
        div[data-testid="stStatusWidget"] {{
            visibility: hidden; 
            height: 0%; 
            position: fixed;
            }}
    
        #MainMenu {{
        visibility: hidden; 
        height: 0%;
        }}
        
        header {{
            visibility: hidden; 
            height:
                0%;
            }}
            
        footer {{
            visibility: hidden; 
            height: 0%;
            }}
        
        div[data-testid="stSpinner"] {{
            color: #000000;
            background-color: #F0F0F0; 
            padding: 10px; 
            border-radius: 5px;
            }}
        
        a[href="#responsive-table"] {{
            visibility: hidden; 
            height: 0%;
            }}
        
        a[href^="#"] {{
            /* Estilos para todos los elementos <a> con href que comienza con "#" */
            visibility: hidden; 
            height: 0%;
            overflow-y: hidden;
        }}

        div[class="table-scroll"] {{
            background-color: #a6c53b;
            visibility: hidden;
            overflow-x: hidden;
            }}
            
        button[data-testid="StyledFullScreenButton"] {{
            visibility: hidden; 
            height: 0%;
        }}
        
        .stButton button {{
                background-color: #105B65;
                font-weight: bold;
                width: 100%;
                border: 2px solid #105B65;
                color:white;
            }}
        
        .stButton button:hover {{
                background-color: #0D4A52;
                font-weight: bold;
                width: 100%;
                border: 2px solid #0D4A52;
                color:white;
        }}
        
        .stButton button:active {{
            background-color: #FFF;
            color: #105B65;
            border: 2px solid #FFF;
            outline: none;
        }}
    
        [data-testid="stMultiSelect"] {{
            border: 5px solid #F0F0F0;
            background-color: #F0F0F0;
            border-radius: 5px;
            padding: 5px; 
        }}
        li[role="option"] > div {{
            font-size: {letra_options};
        }}
        div[data-testid="stMultiSelect"] div[data-baseweb="select"] span {{
            font-size: {letra_options};
        }}
        
        [data-baseweb="select"] > div {{
            background-color: #fff;
        }}
        
        [data-testid="stTextInput"] {{
            border: 5px solid #F0F0F0;
            background-color: #F0F0F0;
            border-radius: 5px;
            padding: 5px;
            font-size: {letra_options};
        }}
    
    
        [data-testid="stSelectbox"] {{
            border: 5px solid #F0F0F0;
            background-color: #F0F0F0;
            border-radius: 5px;
            padding: 5px;  
        }}
        
        button[data-testid="StyledFullScreenButton"] {{
            visibility: hidden; 
            height: 0%;
            
        }}

        [data-testid="stNumberInput"] {{
            border: 5px solid #F0F0F0;
            background-color: #F0F0F0;
            border-radius: 5px;
            padding: 5px;
            font-size: {letra_options};
        }}
        
        [data-baseweb="input"] > div {{
            background-color: #fff;
        }}
        
        div[data-testid="stNumberInput-StepUp"]:hover {{
            background-color: #105B65;
        }}
        
        label[data-testid="stWidgetLabel"] p {{
            font-size: {letra_titulo};
            font-weight: bold;
            color: #3C3840;
            font-family: 'Aptos Narrow';
        }}
        
        span[data-baseweb="tag"] {{
          background-color: #105B65;
        }}
        
        [data-testid="stDateInput"] {{
            border: 5px solid #F0F0F0;
            background-color: #F0F0F0;
            border-radius: 5px;
            padding: 5px; 
        }}
            
        .stDownloadButton button {{
            background-color: #DAE8D8;
            font-weight: bold;
            width: 100%;
            border: 2px solid #DAE8D8;
            color: black;
        }}
        
        .stDownloadButton button:hover {{
            background-color: #DAE8D8;
            color: black;
            border: #DAE8D8;
        }}        

        [data-testid="stNumberInput-Input"]::placeholder {{
            font-size: 8px;
        }}
        
        [data-testid="stTextInput-Input"] {{
            font-size: {letra_options};
        }}

        .stLinkButton button,
        .stLinkButton a {{
            background-color: #FDC62D;
            font-weight: bold;
            width: 100%;
            border: 2px solid #FDC62D;
            color: white;
            transition: background-color 0.2s ease;
        }}
        
        .stLinkButton button:hover,
        .stLinkButton a:hover {{
            background-color: #FDC62D;
            border-color: #FDC62D;
        }}
        
        .stLinkButton button:active,
        .stLinkButton a:active {{
            background-color: #FFF;
            color: #FDC62D;
            border-color: #FFF;
            outline: none;
        }}
        
        /* Estado deshabilitado */
        .stLinkButton button:disabled,
        .stLinkButton a[disabled] {{
            background-color: #cccccc !important;
            color: #666666 !important;
            border-color: #cccccc !important;
            cursor: not-allowed !important;
            opacity: 0.6;
        }}
        
        .stLinkButton button:disabled:hover,
        .stLinkButton a[disabled]:hover {{
            background-color: #cccccc !important;
            border-color: #cccccc !important;
        }}
        
        </style>
        """,
        unsafe_allow_html=True
    )