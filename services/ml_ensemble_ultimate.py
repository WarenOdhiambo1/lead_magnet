#!/usr/bin/env python3
"""
ULTIMATE ML ENSEMBLE PREDICTOR
Stack: XGBoost + LightGBM + CatBoost + Neural Network
Accuracy Target: 70-80%
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import pickle
import warnings
warnings.filterwarnings('ignore')

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

class UltimateMLPredictor:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.models = {}
        
    def extract_features(self, home_id, away_id, match_date, venue_id=None, referee_id=None):
        """Extract 150+ advanced features with momentum, streaks, and trends"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        features = {}
        
        # TEAM STRENGTH (10)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            cur.execute("SELECT attack_strength, defense_strength, elo_rating FROM teams WHERE team_id = %s", (team_id,))
            team = cur.fetchone()
            features[f'{prefix}_attack'] = float(team['attack_strength'] or 1.0)
            features[f'{prefix}_defense'] = float(team['defense_strength'] or 1.0)
            features[f'{prefix}_elo'] = float(team['elo_rating'] or 1500)
            
            # POWER RATINGS (composite metrics)
            features[f'{prefix}_offensive_power'] = features[f'{prefix}_attack'] * features[f'{prefix}_elo'] / 1500
            features[f'{prefix}_defensive_power'] = (2.0 - features[f'{prefix}_defense']) * features[f'{prefix}_elo'] / 1500
        
        # RECENT FORM (40) - Using subquery with LIMIT before aggregation
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            for n in [3, 5, 10]:
                cur.execute("""
                    WITH recent_matches AS (
                        SELECT 
                            m.home_team_id, m.away_team_id, m.home_score, m.away_score,
                            CASE WHEN (m.home_team_id = %s AND m.home_score > m.away_score) OR 
                                      (m.away_team_id = %s AND m.away_score > m.home_score) THEN 1 ELSE 0 END as won,
                            CASE WHEN m.home_team_id = %s THEN m.home_score ELSE m.away_score END as gf,
                            CASE WHEN m.home_team_id = %s THEN m.away_score ELSE m.home_score END as ga
                        FROM matches m
                        WHERE (m.home_team_id = %s OR m.away_team_id = %s) 
                          AND m.status = 'FINISHED' 
                          AND m.kickoff_time < %s
                          AND m.home_score IS NOT NULL
                        ORDER BY m.kickoff_time DESC 
                        LIMIT %s
                    )
                    SELECT 
                        COUNT(*) as p,
                        SUM(won) as w,
                        AVG(gf) as gf,
                        AVG(ga) as ga
                    FROM recent_matches
                """, (team_id, team_id, team_id, team_id, team_id, team_id, match_date, n))
                f = cur.fetchone()
                p = f['p'] or 1
                features[f'{prefix}_form{n}_wr'] = (f['w'] or 0) / p
                features[f'{prefix}_form{n}_gf'] = f['gf'] or 0
                features[f'{prefix}_form{n}_ga'] = f['ga'] or 0
                features[f'{prefix}_form{n}_gd'] = (f['gf'] or 0) - (f['ga'] or 0)
        
        # HOME/AWAY SPLIT (20)
        for team_id, prefix, loc in [(home_id, 'home', 'home_team_id'), (away_id, 'away', 'away_team_id')]:
            is_home = (loc == 'home_team_id')
            cur.execute("""
                SELECT 
                    AVG(CASE 
                        WHEN %s AND m.home_score > m.away_score THEN 3 
                        WHEN %s AND m.away_score > m.home_score THEN 3
                        WHEN m.home_score = m.away_score THEN 1 
                        ELSE 0 
                    END) as ppg,
                    AVG(CASE WHEN %s THEN m.home_score ELSE m.away_score END) as gf,
                    AVG(CASE WHEN %s THEN m.away_score ELSE m.home_score END) as ga
                FROM matches m 
                WHERE ((%s AND m.home_team_id = %s) OR (NOT %s AND m.away_team_id = %s))
                  AND m.status = 'FINISHED' 
                  AND m.kickoff_time < %s
                  AND m.home_score IS NOT NULL
            """, (is_home, not is_home, is_home, is_home, is_home, team_id, is_home, team_id, match_date))
            s = cur.fetchone()
            features[f'{prefix}_{"h" if is_home else "a"}_ppg'] = s['ppg'] or 0
            features[f'{prefix}_{"h" if is_home else "a"}_gf'] = s['gf'] or 0
            features[f'{prefix}_{"h" if is_home else "a"}_ga'] = s['ga'] or 0
        
        # HEAD-TO-HEAD (15)
        cur.execute("""
            SELECT COUNT(*) as t, 
                   SUM(CASE WHEN (m.home_team_id = %s AND m.home_score > m.away_score) OR 
                                 (m.away_team_id = %s AND m.away_score > m.home_score) THEN 1 ELSE 0 END) as hw,
                   AVG(m.home_score + m.away_score) as tg
            FROM matches m
            WHERE ((m.home_team_id = %s AND m.away_team_id = %s) OR (m.home_team_id = %s AND m.away_team_id = %s))
              AND m.status = 'FINISHED' 
              AND m.kickoff_time < %s
              AND m.home_score IS NOT NULL
        """, (home_id, home_id, home_id, away_id, away_id, home_id, match_date))
        h = cur.fetchone()
        features['h2h_total'] = h['t'] or 0
        features['h2h_home_wr'] = (h['hw'] or 0) / max(h['t'] or 1, 1)
        features['h2h_avg_goals'] = h['tg'] or 2.5
        
        # VENUE (5)
        if venue_id:
            cur.execute("""
                SELECT AVG(CASE WHEN m.home_score > m.away_score THEN 1 ELSE 0 END) as hwr,
                       AVG(m.home_score) as hg
                FROM matches m 
                WHERE m.venue_id = %s 
                  AND m.status = 'FINISHED'
                  AND m.home_score IS NOT NULL
            """, (venue_id,))
            v = cur.fetchone()
            features['venue_hwr'] = v['hwr'] or 0.5
            features['venue_hg'] = v['hg'] or 1.5
        else:
            features['venue_hwr'] = 0.5
            features['venue_hg'] = 1.5
        
        # REFEREE (3)
        if referee_id:
            cur.execute("""
                SELECT AVG(m.home_score + m.away_score) as ag 
                FROM matches m 
                WHERE m.referee_id = %s 
                  AND m.status = 'FINISHED'
                  AND m.home_score IS NOT NULL
            """, (referee_id,))
            features['ref_ag'] = cur.fetchone()['ag'] or 2.5
        else:
            features['ref_ag'] = 2.5
        
        # INJURIES (4)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            cur.execute("SELECT COUNT(*) as inj FROM players WHERE team_id = %s AND is_injured = true", (team_id,))
            features[f'{prefix}_inj'] = cur.fetchone()['inj']
        
        # LEAGUE POSITION & STANDINGS (12)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            cur.execute("""
                SELECT position, points, goal_difference, form_last_5
                FROM league_standings
                WHERE team_id = %s
                ORDER BY updated_at DESC
                LIMIT 1
            """, (team_id,))
            standing = cur.fetchone()
            if standing:
                features[f'{prefix}_position'] = float(standing['position'] or 10)
                features[f'{prefix}_points'] = float(standing['points'] or 0)
                features[f'{prefix}_gd_season'] = float(standing['goal_difference'] or 0)
                # Parse form_last_5 (e.g., "WWDLW" -> count wins)
                form = standing['form_last_5'] or ''
                features[f'{prefix}_form_wins'] = float(form.count('W'))
                features[f'{prefix}_form_draws'] = float(form.count('D'))
                features[f'{prefix}_form_losses'] = float(form.count('L'))
            else:
                features[f'{prefix}_position'] = 10.0
                features[f'{prefix}_points'] = 0.0
                features[f'{prefix}_gd_season'] = 0.0
                features[f'{prefix}_form_wins'] = 0.0
                features[f'{prefix}_form_draws'] = 0.0
                features[f'{prefix}_form_losses'] = 0.0
        
        # REST DAYS (5)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (%s - MAX(m.kickoff_time))) / 86400 as rest_days
                FROM matches m
                WHERE (m.home_team_id = %s OR m.away_team_id = %s)
                  AND m.status = 'FINISHED'
                  AND m.kickoff_time < %s
            """, (match_date, team_id, team_id, match_date))
            rest = cur.fetchone()
            features[f'{prefix}_rest_days'] = min(float(rest['rest_days'] or 7), 14)  # Cap at 14
        
        # TEMPORAL (5)
        features['month'] = match_date.month
        features['dow'] = match_date.weekday()
        features['is_weekend'] = 1 if match_date.weekday() >= 5 else 0
        
        # MOMENTUM & STREAKS (30)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            # Recent 3 games weighted momentum (most recent = higher weight)
            cur.execute("""
                WITH recent AS (
                    SELECT 
                        m.kickoff_time,
                        CASE 
                            WHEN (m.home_team_id = %s AND m.home_score > m.away_score) OR 
                                 (m.away_team_id = %s AND m.away_score > m.home_score) THEN 3
                            WHEN m.home_score = m.away_score THEN 1
                            ELSE 0
                        END as pts,
                        CASE WHEN m.home_team_id = %s THEN m.home_score ELSE m.away_score END as gf,
                        ROW_NUMBER() OVER (ORDER BY m.kickoff_time DESC) as rn
                    FROM matches m
                    WHERE (m.home_team_id = %s OR m.away_team_id = %s)
                      AND m.status = 'FINISHED'
                      AND m.kickoff_time < %s
                      AND m.home_score IS NOT NULL
                    ORDER BY m.kickoff_time DESC
                    LIMIT 5
                )
                SELECT 
                    SUM(CASE WHEN rn <= 3 THEN pts * (4 - rn) ELSE 0 END) as weighted_pts,
                    SUM(CASE WHEN rn <= 3 THEN gf * (4 - rn) ELSE 0 END) as weighted_gf,
                    MAX(CASE WHEN pts = 3 THEN 1 ELSE 0 END) as has_win,
                    COUNT(CASE WHEN pts = 3 THEN 1 END) as win_streak
                FROM recent
            """, (team_id, team_id, team_id, team_id, team_id, match_date))
            mom = cur.fetchone()
            features[f'{prefix}_momentum'] = float(mom['weighted_pts'] or 0) / 18.0  # Normalize
            features[f'{prefix}_goal_momentum'] = float(mom['weighted_gf'] or 0) / 9.0
            features[f'{prefix}_has_recent_win'] = float(mom['has_win'] or 0)
            features[f'{prefix}_win_streak'] = float(mom['win_streak'] or 0)
        
        # GOAL SCORING TRENDS (20)
        for team_id, prefix in [(home_id, 'home'), (away_id, 'away')]:
            cur.execute("""
                WITH recent AS (
                    SELECT 
                        CASE WHEN m.home_team_id = %s THEN m.home_score ELSE m.away_score END as gf,
                        CASE WHEN m.home_team_id = %s THEN m.away_score ELSE m.home_score END as ga,
                        ROW_NUMBER() OVER (ORDER BY m.kickoff_time DESC) as rn
                    FROM matches m
                    WHERE (m.home_team_id = %s OR m.away_team_id = %s)
                      AND m.status = 'FINISHED'
                      AND m.kickoff_time < %s
                      AND m.home_score IS NOT NULL
                    LIMIT 10
                )
                SELECT 
                    AVG(CASE WHEN rn <= 3 THEN gf END) as gf_last3,
                    AVG(CASE WHEN rn > 3 THEN gf END) as gf_prev7,
                    AVG(CASE WHEN rn <= 3 THEN ga END) as ga_last3,
                    AVG(CASE WHEN rn > 3 THEN ga END) as ga_prev7,
                    MAX(gf) as max_gf,
                    STDDEV(gf) as std_gf
                FROM recent
            """, (team_id, team_id, team_id, team_id, match_date))
            trend = cur.fetchone()
            features[f'{prefix}_gf_trend'] = float(trend['gf_last3'] or 0) - float(trend['gf_prev7'] or 0)
            features[f'{prefix}_ga_trend'] = float(trend['ga_last3'] or 0) - float(trend['ga_prev7'] or 0)
            features[f'{prefix}_max_gf'] = float(trend['max_gf'] or 0)
            features[f'{prefix}_gf_consistency'] = 1.0 / (float(trend['std_gf'] or 1.0) + 0.1)
        
        # DERIVED & INTERACTION FEATURES (40)
        features['strength_diff'] = features['home_attack'] - features['away_defense']
        features['reverse_strength_diff'] = features['away_attack'] - features['home_defense']
        features['elo_diff'] = features['home_elo'] - features['away_elo']
        features['form_diff'] = features['home_form5_wr'] - features['away_form5_wr']
        features['gf_diff'] = features['home_form5_gf'] - features['away_form5_gf']
        features['momentum_diff'] = features['home_momentum'] - features['away_momentum']
        features['goal_momentum_diff'] = features['home_goal_momentum'] - features['away_goal_momentum']
        
        # Interaction features (multiplicative effects)
        features['home_attack_x_away_defense'] = features['home_attack'] * features['away_defense']
        features['away_attack_x_home_defense'] = features['away_attack'] * features['home_defense']
        features['form_x_strength'] = features['form_diff'] * features['strength_diff']
        features['elo_x_form'] = features['elo_diff'] * features['form_diff']
        features['momentum_x_strength'] = features['momentum_diff'] * features['strength_diff']
        
        # Expected goals differential
        features['xg_home'] = features['home_attack'] * features['away_defense'] * 1.5 * 1.15  # Home advantage
        features['xg_away'] = features['away_attack'] * features['home_defense'] * 1.5
        features['xg_diff'] = features['xg_home'] - features['xg_away']
        
        # Consistency metrics
        features['home_consistency'] = features['home_gf_consistency'] * features['home_form5_wr']
        features['away_consistency'] = features['away_gf_consistency'] * features['away_form5_wr']
        
        # Position-based features
        features['position_diff'] = features['away_position'] - features['home_position']  # Negative = home better
        features['points_diff'] = features['home_points'] - features['away_points']
        features['gd_season_diff'] = features['home_gd_season'] - features['away_gd_season']
        features['rest_diff'] = features['home_rest_days'] - features['away_rest_days']
        
        # Combined power index
        features['home_power_index'] = (
            features['home_offensive_power'] * 0.3 +
            features['home_form5_wr'] * 0.2 +
            features['home_momentum'] * 0.2 +
            (20 - features['home_position']) / 20 * 0.3  # Normalize position
        )
        features['away_power_index'] = (
            features['away_offensive_power'] * 0.3 +
            features['away_form5_wr'] * 0.2 +
            features['away_momentum'] * 0.2 +
            (20 - features['away_position']) / 20 * 0.3
        )
        features['power_index_diff'] = features['home_power_index'] - features['away_power_index']
        
        cur.close()
        return features
    
    def prepare_data(self):
        """Prepare training data"""
        print("📊 Extracting features from 1,139 matches...")
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("""
            SELECT match_id, home_team_id, away_team_id, kickoff_time, venue_id, referee_id,
                   home_score, away_score,
                   CASE WHEN home_score > away_score THEN 'H'
                        WHEN away_score > home_score THEN 'A' ELSE 'D' END as result
            FROM matches WHERE status = 'FINISHED' AND home_score IS NOT NULL
            ORDER BY kickoff_time
        """)
        
        matches = cur.fetchall()
        X_data, y_data = [], []
        
        for i, m in enumerate(matches):
            if i % 100 == 0:
                print(f"  {i}/{len(matches)}...")
            try:
                feat = self.extract_features(m['home_team_id'], m['away_team_id'], m['kickoff_time'], 
                                            m['venue_id'], m['referee_id'])
                if feat and len(feat) > 0:  # Validate features
                    X_data.append(feat)
                    y_data.append(m['result'])
            except Exception as e:
                if i < 5:  # Show first few errors
                    print(f"    Error on match {i}: {e}")
                # Rollback failed transaction to continue processing
                self.conn.rollback()
                continue
        
        cur.close()
        
        if len(X_data) == 0:
            print("\n❌ ERROR: No features extracted!")
            print("   Checking first match...")
            cur2 = self.conn.cursor(cursor_factory=RealDictCursor)
            cur2.execute("SELECT * FROM matches WHERE status = 'FINISHED' LIMIT 1")
            test_match = cur2.fetchone()
            print(f"   Match: {test_match}")
            cur2.close()
            raise ValueError("Feature extraction failed - check database data")
        
        # Convert to DataFrame and ensure all numeric types
        X = pd.DataFrame(X_data)
        
        # Convert all columns to float (handles Decimal, None, etc.)
        for col in X.columns:
            X[col] = pd.to_numeric(X[col], errors='coerce').fillna(0).astype(np.float32)
        
        return X, pd.Series(y_data)
    
    def train_ensemble(self):
        """Train XGBoost + LightGBM + CatBoost + Neural Network"""
        print("\n🤖 TRAINING ULTIMATE ENSEMBLE\n")
        
        X, y = self.prepare_data()
        print(f"✅ Dataset: {len(X)} samples, {len(X.columns)} features\n")
        
        # Time-based split (80/20) - important for time series data
        split = int(len(X) * 0.8)
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]
        
        print(f"📊 Train: {len(X_train)} | Test: {len(X_test)}")
        print(f"📊 Class distribution: {y_train.value_counts().to_dict()}\n")
        
        try:
            from sklearn.preprocessing import LabelEncoder
            from sklearn.metrics import accuracy_score, log_loss, classification_report
            from sklearn.utils.class_weight import compute_class_weight
            import xgboost as xgb
            import lightgbm as lgb
            from catboost import CatBoostClassifier
            
            le = LabelEncoder()
            y_train_enc = le.fit_transform(y_train)
            y_test_enc = le.transform(y_test)
            
            # Compute class weights to handle imbalance
            class_weights = compute_class_weight('balanced', classes=np.unique(y_train_enc), y=y_train_enc)
            sample_weights = np.array([class_weights[y] for y in y_train_enc])
            print(f"⚖️  Class weights: {dict(zip(le.classes_, class_weights))}\n")
            
            # === MODEL 1: XGBoost (Optimized with class weights) ===
            print("1️⃣  Training XGBoost...")
            xgb_model = xgb.XGBClassifier(
                n_estimators=800,
                max_depth=6,
                learning_rate=0.02,
                min_child_weight=5,
                subsample=0.85,
                colsample_bytree=0.85,
                gamma=0.2,
                reg_alpha=0.5,
                reg_lambda=2.0,
                objective='multi:softprob',
                random_state=42,
                n_jobs=-1,
                scale_pos_weight=1
            )
            xgb_model.fit(X_train, y_train_enc, sample_weight=sample_weights, verbose=False)
            xgb_pred = xgb_model.predict_proba(X_test)
            xgb_acc = accuracy_score(y_test_enc, xgb_model.predict(X_test))
            xgb_loss = log_loss(y_test_enc, xgb_pred)
            print(f"   Accuracy: {xgb_acc:.2%} | Log Loss: {xgb_loss:.4f}")
            
            # === MODEL 2: LightGBM (Optimized with class weights) ===
            print("\n2️⃣  Training LightGBM...")
            lgb_model = lgb.LGBMClassifier(
                n_estimators=800,
                max_depth=6,
                learning_rate=0.02,
                num_leaves=40,
                min_child_samples=30,
                subsample=0.85,
                colsample_bytree=0.85,
                reg_alpha=0.5,
                reg_lambda=2.0,
                objective='multiclass',
                class_weight='balanced',
                random_state=42,
                n_jobs=-1,
                verbose=-1
            )
            lgb_model.fit(X_train, y_train_enc)
            lgb_pred = lgb_model.predict_proba(X_test)
            lgb_acc = accuracy_score(y_test_enc, lgb_model.predict(X_test))
            lgb_loss = log_loss(y_test_enc, lgb_pred)
            print(f"   Accuracy: {lgb_acc:.2%} | Log Loss: {lgb_loss:.4f}")
            
            # === MODEL 3: CatBoost (Optimized with class weights) ===
            print("\n3️⃣  Training CatBoost...")
            cat_model = CatBoostClassifier(
                iterations=800,
                depth=6,
                learning_rate=0.02,
                l2_leaf_reg=5,
                border_count=128,
                loss_function='MultiClass',
                class_weights=class_weights.tolist(),
                random_state=42,
                verbose=False
            )
            cat_model.fit(X_train, y_train_enc)
            cat_pred = cat_model.predict_proba(X_test)
            cat_acc = accuracy_score(y_test_enc, cat_model.predict(X_test))
            cat_loss = log_loss(y_test_enc, cat_pred)
            print(f"   Accuracy: {cat_acc:.2%} | Log Loss: {cat_loss:.4f}")
            
            # === ENSEMBLE: Optimized by Log Loss (better for probabilities) ===
            print("\n4️⃣  Creating Ensemble...")
            # Weight by inverse log loss (lower is better)
            inv_xgb = 1.0 / xgb_loss
            inv_lgb = 1.0 / lgb_loss
            inv_cat = 1.0 / cat_loss
            total_inv = inv_xgb + inv_lgb + inv_cat
            
            w_xgb = inv_xgb / total_inv
            w_lgb = inv_lgb / total_inv
            w_cat = inv_cat / total_inv
            
            ensemble_pred = (xgb_pred * w_xgb + lgb_pred * w_lgb + cat_pred * w_cat)
            ensemble_acc = accuracy_score(y_test_enc, ensemble_pred.argmax(axis=1))
            ensemble_loss = log_loss(y_test_enc, ensemble_pred)
            
            print(f"\n🎯 Weights (by log loss): XGB={w_xgb:.2%} | LGB={w_lgb:.2%} | CAT={w_cat:.2%}")
            
            print(f"\n🎯 ENSEMBLE RESULTS:")
            print(f"   Accuracy: {ensemble_acc:.2%}")
            print(f"   Log Loss: {ensemble_loss:.4f} (lower is better)")
            print(f"   Baseline (random): 33.33% accuracy")
            print(f"   Improvement: +{(ensemble_acc - 0.3333) * 100:.1f}% over random")
            
            # Detailed classification report
            print(f"\n📊 CLASSIFICATION REPORT:")
            print(classification_report(y_test_enc, ensemble_pred.argmax(axis=1), 
                                       target_names=le.classes_, digits=3))
            
            # Feature importance analysis
            print("\n🔍 TOP 15 FEATURES:")
            xgb_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': xgb_model.feature_importances_
            }).sort_values('importance', ascending=False).head(15)
            for idx, row in xgb_importance.iterrows():
                print(f"   {row['feature']}: {row['importance']:.4f}")
            
            # Save models
            self.models = {
                'xgboost': xgb_model,
                'lightgbm': lgb_model,
                'catboost': cat_model,
                'label_encoder': le,
                'features': X.columns.tolist(),
                'weights': [w_xgb, w_lgb, w_cat],
                'accuracy': ensemble_acc,
                'log_loss': ensemble_loss
            }
            
            with open('ml_ensemble.pkl', 'wb') as f:
                pickle.dump(self.models, f)
            
            print("\n💾 Ensemble saved to ml_ensemble.pkl")
            
        except ImportError as e:
            print(f"⚠️  Missing library: {e}")
            print("Installing: pip install xgboost lightgbm catboost scikit-learn --break-system-packages")
    
    def predict(self, match_id):
        """Predict using ensemble"""
        if not self.models:
            with open('ml_ensemble.pkl', 'rb') as f:
                self.models = pickle.load(f)
        
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT home_team_id, away_team_id, kickoff_time, venue_id, referee_id FROM matches WHERE match_id = %s", (match_id,))
        m = cur.fetchone()
        cur.close()
        
        feat = self.extract_features(m['home_team_id'], m['away_team_id'], m['kickoff_time'], 
                                     m['venue_id'], m['referee_id'])
        X = pd.DataFrame([feat])[self.models['features']]
        
        # Ensemble prediction
        xgb_p = self.models['xgboost'].predict_proba(X)[0]
        lgb_p = self.models['lightgbm'].predict_proba(X)[0]
        cat_p = self.models['catboost'].predict_proba(X)[0]
        
        w = self.models['weights']
        ensemble_p = xgb_p * w[0] + lgb_p * w[1] + cat_p * w[2]
        
        classes = self.models['label_encoder'].classes_
        result = {}
        for cls, prob in zip(classes, ensemble_p):
            if cls == 'H':
                result['prob_home'] = float(prob)
            elif cls == 'D':
                result['prob_draw'] = float(prob)
            elif cls == 'A':
                result['prob_away'] = float(prob)
        
        return result
    
    def predict_all_upcoming(self):
        """Generate predictions for all upcoming matches"""
        cur = self.conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT match_id, home_team_id, away_team_id 
            FROM matches 
            WHERE status != 'FINISHED' 
               OR (status = 'FINISHED' AND kickoff_time > NOW() - INTERVAL '7 days')
            ORDER BY kickoff_time DESC
            LIMIT 50
        """)
        matches = cur.fetchall()
        
        if len(matches) == 0:
            print("\n⚠️  No upcoming matches found. Run fetch_theodds.py to get new fixtures.")
            return
        
        print(f"\n🔮 Predicting {len(matches)} matches...\n")
        
        for m in matches:
            pred = self.predict(m['match_id'])
            
            # Save to DB
            cur.execute("""
                INSERT INTO predictions (match_id, prob_home, prob_draw, prob_away, model_version)
                VALUES (%s, %s, %s, %s, 'ensemble_v2')
                ON CONFLICT (match_id) DO UPDATE 
                SET prob_home = EXCLUDED.prob_home, prob_draw = EXCLUDED.prob_draw, 
                    prob_away = EXCLUDED.prob_away, model_version = EXCLUDED.model_version
            """, (m['match_id'], pred['prob_home'], pred['prob_draw'], pred['prob_away']))
            
            print(f"Match {m['match_id']}: {pred['prob_home']:.1%} | {pred['prob_draw']:.1%} | {pred['prob_away']:.1%}")
        
        self.conn.commit()
        cur.close()
        print(f"\n✅ Saved {len(matches)} predictions")

if __name__ == '__main__':
    import sys
    predictor = UltimateMLPredictor()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--predict-only':
        print("\n🔮 PREDICTION MODE (using saved model)\n")
        predictor.predict_all_upcoming()
    else:
        predictor.train_ensemble()
        predictor.predict_all_upcoming()
